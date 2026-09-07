// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#![cfg(test)]

use http::Uri;
use neqo_common::{Datagram, Tos, event::Provider as _, header::HeadersExt as _, qinfo};
use neqo_http3::{
    ConnectUdpEvent, Error, Http3Client, Http3ClientEvent, Http3Parameters, Http3Server,
    Http3ServerEvent, Http3State, Priority, SessionAcceptAction,
    connect_udp::{ClientSession as _, ServerEvent, ServerSession},
    webtransport::ClientSession as _,
};
use neqo_transport::{ConnectionParameters, StreamType};
use nss::AuthenticationStatus;
use test_fixture::{
    DEFAULT_ADDR, default_http3_client, default_http3_server, exchange_packets, fixture_init,
    http3_client_with_params, http3_server_with_params, now,
};

const PING: &[u8] = b"ping";
const PONG: &[u8] = b"pong";

#[test]
fn disabled_by_default() {
    let mut client = default_http3_client();
    let mut server = default_http3_server();
    // Connect client and proxy.
    let _out = test_fixture::connect_peers(&mut client, &mut server);
    assert!(!client.connect_udp_enabled());
}

fn initiate_new_session() -> (Http3Client, Http3Server, neqo_http3::StreamId) {
    initiate_new_session_with_client_params(
        ConnectionParameters::default()
            .pmtud(true)
            .datagram_size(1500),
    )
}

fn initiate_new_session_with_client_params(
    client_conn_params: ConnectionParameters,
) -> (Http3Client, Http3Server, neqo_http3::StreamId) {
    let proxy_conn_params = ConnectionParameters::default()
        .pmtud(true)
        .datagram_size(1500);

    let mut client = http3_client_with_params(
        Http3Parameters::default()
            .connect(true)
            .connection_parameters(client_conn_params),
    );

    let mut proxy = http3_server_with_params(
        Http3Parameters::default()
            .connect(true)
            .connection_parameters(proxy_conn_params),
    );

    // Connect client and proxy.
    let out = test_fixture::connect_peers(&mut client, &mut proxy);
    let out = proxy.process(out, now()).dgram().unwrap();
    client.process_input(out, now());
    assert!(client.connect_udp_enabled());

    // Establish connect-udp session.
    let connect_udp_session_id = client
        .connect_udp_create_session(
            now(),
            &format!("https://[{}]:{}/", DEFAULT_ADDR.ip(), DEFAULT_ADDR.port())
                .parse::<Uri>()
                .unwrap(),
            &[],
        )
        .unwrap();
    (client, proxy, connect_udp_session_id)
}

fn establish_new_session() -> (
    Http3Client,
    Http3Server,
    neqo_http3::StreamId,
    ServerSession,
) {
    establish_new_session_with_client_params(
        ConnectionParameters::default()
            .pmtud(true)
            .datagram_size(1500),
    )
}

fn establish_new_session_with_client_params(
    client_conn_params: ConnectionParameters,
) -> (
    Http3Client,
    Http3Server,
    neqo_http3::StreamId,
    ServerSession,
) {
    let (mut client, mut proxy, connect_udp_session_id) =
        initiate_new_session_with_client_params(client_conn_params);
    exchange_packets(&mut client, &mut proxy, false, None);
    let proxy_session = proxy
        .events()
        .find_map(|event| {
            if let Http3ServerEvent::ConnectUdp(ServerEvent::NewSession { session, headers }) =
                event
            {
                assert_eq!(session.stream_id(), connect_udp_session_id);

                assert!(
                    headers.contains_header(":method", "CONNECT")
                        && headers.contains_header(":protocol", "connect-udp")
                        && headers.contains_header("capsule-protocol", "?1")
                );

                session
                    .response(&SessionAcceptAction::Accept, now())
                    .unwrap();
                Some(session)
            } else {
                None
            }
        })
        .unwrap();
    exchange_packets(&mut client, &mut proxy, false, None);
    client
        .events()
        .find(|e| matches!(
            e,
            Http3ClientEvent::ConnectUdp(ConnectUdpEvent::NewSession { stream_id, status, ..}) if *stream_id == connect_udp_session_id && *status == 200)
        )
        .unwrap();
    (client, proxy, connect_udp_session_id, proxy_session)
}

fn exchange_packets_through_proxy(
    client_outer: &mut Http3Client,
    client_inner: &mut Http3Client,
    proxy: &mut Http3Server,
    server: &mut Http3Server,
    connect_udp_session_id: neqo_http3::StreamId,
    proxy_session: &ServerSession,
) {
    qinfo!("Processing client_inner");
    while let Some(dgram) = client_inner.process_output(now()).dgram() {
        client_outer
            .connect_udp_send_datagram(connect_udp_session_id, dgram.as_ref(), None, now())
            .unwrap();
    }

    qinfo!("Processing client_outer");
    let mut client_outer_dgrams = client_outer
        .process_multiple_output(now(), 64.try_into().unwrap())
        .dgram()
        .unwrap();

    qinfo!("Processing proxy");
    let proxy_out = proxy
        .process_multiple(
            client_outer_dgrams.iter_mut(),
            now(),
            64.try_into().unwrap(),
        )
        .dgram();
    if let Some(mut dgram) = proxy_out {
        client_outer.process_multiple_input(dgram.iter_mut(), now());
    }
    let server_dgrams = proxy.events().filter_map(|event| match event {
        Http3ServerEvent::ConnectUdp(ServerEvent::Datagram { datagram, session }) => {
            assert_eq!(session.stream_id(), connect_udp_session_id);
            Some(Datagram::from_bytes(
                DEFAULT_ADDR,
                DEFAULT_ADDR,
                Tos::default(),
                datagram,
            ))
        }
        _ => None,
    });

    qinfo!("Processing server");
    let mut server_out = vec![];
    for dgram in server_dgrams {
        if let Some(dgram) = server.process(Some(dgram), now()).dgram() {
            server_out.push(dgram);
        }
    }
    while let Some(dgram) = server.process(Option::<Datagram>::None, now()).dgram() {
        server_out.push(dgram);
    }

    qinfo!("Processing proxy");
    for dgram in server_out {
        proxy_session
            .send_datagram(dgram.as_ref(), None, now())
            .unwrap();
    }
    let mut proxy_out = vec![];
    while let Some(dgram) = proxy.process(Vec::<Datagram>::new(), now()).dgram() {
        proxy_out.push(dgram);
    }

    qinfo!("Processing client_outer");
    client_outer.process_multiple_input(proxy_out, now());

    qinfo!("Processing client_inner");
    let client_inner_dgrams = client_outer.events().filter_map(|event| {
        if let Http3ClientEvent::ConnectUdp(ConnectUdpEvent::Datagram {
            session_id,
            datagram,
        }) = event
        {
            assert_eq!(session_id, connect_udp_session_id);
            Some(Datagram::from_bytes(
                DEFAULT_ADDR,
                DEFAULT_ADDR,
                Tos::default(),
                datagram,
            ))
        } else {
            None
        }
    });
    client_inner.process_multiple_input(client_inner_dgrams, now());
}

fn session_lifecycle(client_closes: bool) {
    fixture_init();
    neqo_common::log::init(None);

    let (mut client, mut proxy, session_id, proxy_session) = establish_new_session();

    client
        .connect_udp_send_datagram(session_id, PING, None, now())
        .unwrap();

    exchange_packets(&mut client, &mut proxy, false, None);

    let (id, datagram) = proxy
        .events()
        .find_map(|event| {
            if let Http3ServerEvent::ConnectUdp(ServerEvent::Datagram { session, datagram }) = event
            {
                Some((session.stream_id(), datagram))
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(session_id, id);
    assert_eq!(&datagram, PING);

    proxy_session.send_datagram(PONG, None, now()).unwrap();

    exchange_packets(&mut client, &mut proxy, false, None);

    let (id, datagram) = client
        .events()
        .find_map(|event| {
            if let Http3ClientEvent::ConnectUdp(ConnectUdpEvent::Datagram {
                session_id: id,
                datagram,
            }) = event
            {
                Some((id, datagram))
            } else {
                None
            }
        })
        .unwrap();

    assert_eq!(session_id, id);
    assert_eq!(&datagram, PONG);

    if client_closes {
        client
            .connect_udp_close_session(session_id, 0, "kthxbye", now())
            .unwrap();

        exchange_packets(&mut client, &mut proxy, false, None);

        proxy
            .events()
            .find(|event| {
                matches!(
                    event,
                    Http3ServerEvent::ConnectUdp(ServerEvent::SessionClosed {
                        session,
                        ..
                    }) if session.stream_id() == session_id
                )
            })
            .unwrap();
    } else {
        proxy_session.close_session(0, "kthxbye", now()).unwrap();

        exchange_packets(&mut client, &mut proxy, false, None);

        client
            .events()
            .find(|event| {
                matches!(
                    event,
                    Http3ClientEvent::ConnectUdp(ConnectUdpEvent::SessionClosed {
                        stream_id,
                        ..
                    }) if *stream_id == session_id
                )
            })
            .unwrap();
    }
}

#[test]
fn session_lifecycle_client_closes() {
    session_lifecycle(true);
}

#[test]
fn session_lifecycle_server_closes() {
    session_lifecycle(false);
}

#[test]
fn connect_via_proxy() {
    fixture_init();
    neqo_common::log::init(None);

    let mut client_inner = default_http3_client();
    let mut server = default_http3_server();

    let (mut client_outer, mut proxy, connect_udp_session_id, proxy_session) =
        establish_new_session();

    let mut needs_auth = false;
    // Establish inner connection on top of connect-udp session.
    'outer: loop {
        for event in client_inner.events() {
            match event {
                Http3ClientEvent::AuthenticationNeeded => {
                    needs_auth = true;
                }
                Http3ClientEvent::StateChange(Http3State::Connected) => break 'outer,
                _ => {}
            }
        }

        if needs_auth {
            client_inner.authenticated(AuthenticationStatus::Ok, now());
            needs_auth = false;
        }

        exchange_packets_through_proxy(
            &mut client_outer,
            &mut client_inner,
            &mut proxy,
            &mut server,
            connect_udp_session_id,
            &proxy_session,
        );
    }

    client_inner.close(now(), 0, "kthxbye");

    'outer: loop {
        for event in server.events() {
            if let Http3ServerEvent::StateChange {
                state: Http3State::Closing(_),
                ..
            } = event
            {
                break 'outer;
            }
        }

        exchange_packets_through_proxy(
            &mut client_outer,
            &mut client_inner,
            &mut proxy,
            &mut server,
            connect_udp_session_id,
            &proxy_session,
        );
    }
}

#[test]
fn send_dgram_on_non_active_session() {
    let (mut client, _proxy, connect_udp_session_id) = initiate_new_session();

    assert_eq!(
        client.connect_udp_send_datagram(connect_udp_session_id, &[], None, now()),
        Err(Error::InvalidStreamId)
    );
}

/// A server datagram, arriving before the server accepted the session, is dropped.
#[test]
fn server_datagram_before_accept() {
    for in_order in [true, false] {
        let (mut client, mut proxy, _connect_udp_session_id) = initiate_new_session();
        exchange_packets(&mut client, &mut proxy, false, None);

        let proxy_session = proxy
            .events()
            .find_map(|event| {
                if let Http3ServerEvent::ConnectUdp(ServerEvent::NewSession { session, .. }) = event
                {
                    Some(session)
                } else {
                    None
                }
            })
            .unwrap();
        proxy_session
            .response(&SessionAcceptAction::Accept, now())
            .unwrap();
        let proxy_accept = proxy.process_output(now()).dgram().unwrap();
        assert!(proxy.process_output(now()).dgram().is_none());

        proxy_session.send_datagram(b"ping", None, now()).unwrap();
        let proxy_dgram = proxy.process_output(now()).dgram().unwrap();

        while client.next_event().is_some() {}

        if in_order {
            client.process_input(proxy_accept, now());
            assert!(matches!(
                client.events().next(),
                Some(Http3ClientEvent::ConnectUdp(
                    ConnectUdpEvent::NewSession { .. }
                ))
            ));
            client.process_input(proxy_dgram, now());
            assert!(matches!(
                client.events().next(),
                Some(Http3ClientEvent::ConnectUdp(
                    ConnectUdpEvent::Datagram { .. }
                ))
            ));
        } else {
            client.process_input(proxy_dgram, now());
            assert_eq!(client.events().next(), None,);
            client.process_input(proxy_accept, now());
            assert!(matches!(
                client.events().next(),
                Some(Http3ClientEvent::ConnectUdp(
                    ConnectUdpEvent::NewSession { .. }
                ))
            ));
            assert_eq!(client.events().next(), None,);
        }
    }
}

#[test]
fn create_session_without_connect_setting() {
    let mut client = http3_client_with_params(Http3Parameters::default().connect(false));
    assert_eq!(
        client.connect_udp_create_session(now(), &Uri::from_static("https://example.com/"), &[]),
        Err(Error::Unavailable)
    );
}

#[test]
fn server_stream_reset_results_in_client_session_close() {
    let (mut client, mut proxy, _connect_udp_session_id) = initiate_new_session();
    exchange_packets(&mut client, &mut proxy, false, None);

    while client.next_event().is_some() {}

    let proxy_session = proxy
        .events()
        .find_map(|event| {
            if let Http3ServerEvent::ConnectUdp(ServerEvent::NewSession { session, .. }) = event {
                Some(session)
            } else {
                None
            }
        })
        .unwrap();

    proxy_session.reset_send().unwrap();
    exchange_packets(&mut client, &mut proxy, false, None);

    assert!(matches!(
        client.next_event(),
        Some(Http3ClientEvent::ConnectUdp(
            ConnectUdpEvent::SessionClosed { .. }
        ))
    ));
}

#[test]
fn connect_udp_operation_on_fetch_stream() {
    let (mut client, _proxy, _session_id, _proxy_session) = establish_new_session();
    let fetch_stream = client
        .fetch(
            now(),
            "GET",
            ("https", "something.com", "/"),
            &[],
            Priority::default(),
        )
        .unwrap();

    assert_eq!(
        client.connect_udp_send_datagram(fetch_stream, PING, None, now()),
        Err(Error::InvalidStreamId)
    );

    assert_eq!(
        client.connect_udp_close_session(fetch_stream, 0, "kthxbye", now()),
        Err(Error::InvalidStreamId)
    );
}

#[test]
fn session_lifecycle_with_http_datagram_capsule() {
    let (mut client, mut proxy, session_id, proxy_session) = establish_capsule_session(None, None);

    qinfo!("Testing Capsule send (client -> server)");
    client
        .connect_udp_send_datagram(session_id, PING, None, now())
        .unwrap();

    exchange_packets(&mut client, &mut proxy, false, None);

    let (id, datagram) = proxy
        .events()
        .find_map(|event| {
            if let Http3ServerEvent::ConnectUdp(ServerEvent::Datagram { session, datagram }) = event
            {
                Some((session.stream_id(), datagram))
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(session_id, id);
    assert_eq!(&datagram, PING);
    qinfo!("Capsule decode successful (client -> server)");

    qinfo!("Testing Capsule receive (server -> client)");
    proxy_session.send_datagram(PONG, None, now()).unwrap();

    exchange_packets(&mut client, &mut proxy, false, None);

    let (id, datagram) = client
        .events()
        .find_map(|event| {
            if let Http3ClientEvent::ConnectUdp(ConnectUdpEvent::Datagram {
                session_id: id,
                datagram,
            }) = event
            {
                Some((id, datagram))
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(session_id, id);
    assert_eq!(&datagram, PONG);
    qinfo!("Capsule encode/decode successful (server -> client)");

    qinfo!("Testing multiple datagrams via Capsules");
    for i in 0..5 {
        let mut payload = PING.to_vec();
        payload.push(i);
        client
            .connect_udp_send_datagram(session_id, &payload, None, now())
            .unwrap();
    }

    exchange_packets(&mut client, &mut proxy, false, None);

    let mut count = 0;
    for event in proxy.events() {
        if let Http3ServerEvent::ConnectUdp(ServerEvent::Datagram { session, datagram }) = event {
            assert_eq!(session.stream_id(), session_id);
            assert_eq!(&datagram.as_ref()[..4], PING);
            count += 1;
        }
    }
    assert_eq!(count, 5, "Should receive all 5 datagrams via Capsules");
    qinfo!("Multiple Capsules transmitted successfully");

    client
        .connect_udp_close_session(session_id, 0, "capsule test complete", now())
        .unwrap();

    exchange_packets(&mut client, &mut proxy, false, None);

    proxy
        .events()
        .find(|event| {
            matches!(
                event,
                Http3ServerEvent::ConnectUdp(ServerEvent::SessionClosed {
                    session,
                    ..
                }) if session.stream_id() == session_id
            )
        })
        .unwrap();

    assert_eq!(
        client.transport_stats().frame_tx.datagram,
        0,
        "No QUIC datagram frames should have been sent by client"
    );

    qinfo!("HTTP DATAGRAM Capsule test completed successfully");
}

#[test]
fn connect_udp_session_protocol_is_not_webtransport() {
    fixture_init();
    let (mut client, _proxy, session_id, _proxy_session) = establish_new_session();
    assert_eq!(
        client.webtransport_session_protocol(session_id).unwrap(),
        None,
    );
    assert_eq!(
        client.stream_commit(session_id, now()),
        Err(Error::Unavailable),
        "commit() not implemented for CONNECT-UDP"
    );
}

/// `webtransport_session_stats` must not return stats for a connect-udp
/// session just because it happens to share the extended-CONNECT session
/// machinery with WebTransport.
#[test]
fn connect_udp_session_has_no_webtransport_stats() {
    fixture_init();
    let (client, _proxy, session_id, _proxy_session) = establish_new_session();
    assert_eq!(
        client.webtransport_session_stats(session_id),
        Err(Error::InvalidStreamId)
    );
}

/// `webtransport_close_session` must not accept a connect-udp session id
/// just because it happens to share the extended-CONNECT session
/// machinery with WebTransport.
#[test]
fn connect_udp_session_rejected_by_webtransport_close_session() {
    fixture_init();
    let (mut client, _proxy, session_id, _proxy_session) = establish_new_session();
    assert_eq!(
        client.webtransport_close_session(session_id, 0, "", now()),
        Err(Error::InvalidStreamId)
    );
}

/// `webtransport_create_stream` must not accept a connect-udp session id
/// just because it happens to share the extended-CONNECT session
/// machinery with WebTransport.
#[test]
fn connect_udp_session_rejected_by_webtransport_create_stream() {
    fixture_init();
    let (mut client, _proxy, session_id, _proxy_session) = establish_new_session();
    assert_eq!(
        client.webtransport_create_stream(session_id, StreamType::UniDi),
        Err(Error::InvalidStreamId)
    );
}

/// Backpressure surfaces end-to-end through connect-udp: once
/// `connect_udp_send_datagram` fills the outgoing QUIC datagram queue and
/// returns `Ok(false)`, draining it must deliver
/// [`OutgoingDatagramSpaceAvailable`], so a datagram sender that backs off on
/// `Ok(false)` learns it can resume.
///
/// [`OutgoingDatagramSpaceAvailable`]: neqo_http3::Http3ClientEvent::OutgoingDatagramSpaceAvailable
#[test]
fn outgoing_datagram_space_available_forwarded() {
    fixture_init();
    let (mut client, mut proxy, session_id, _proxy_session) =
        establish_new_session_with_client_params(
            ConnectionParameters::default()
                .pmtud(true)
                .datagram_size(1500)
                .outgoing_datagram_queue(1),
        );

    // Drain session-setup events so the assertions below only observe the
    // datagram backpressure signal.
    while client.next_event().is_some() {}

    assert_eq!(
        client.connect_udp_send_datagram(session_id, PING, None, now()),
        Ok(false)
    );
    assert!(
        !client
            .events()
            .any(|e| matches!(e, Http3ClientEvent::OutgoingDatagramSpaceAvailable)),
        "resume event fired before the queue drained"
    );

    exchange_packets(&mut client, &mut proxy, false, None);
    assert!(
        client
            .events()
            .any(|e| matches!(e, Http3ClientEvent::OutgoingDatagramSpaceAvailable)),
        "OutgoingDatagramSpaceAvailable was not forwarded through connect-udp"
    );
}

/// Establish a connect-udp session over the HTTP DATAGRAM Capsule path, granting
/// the client only `proxy_max_stream_data` bytes of control-stream flow control.
fn establish_capsule_session(
    proxy_max_stream_data: Option<u64>,
    client_max_stream_data: Option<u64>,
) -> (
    Http3Client,
    Http3Server,
    neqo_http3::StreamId,
    ServerSession,
) {
    fixture_init();
    neqo_common::log::init(None);

    // `datagram_size(0)` forces the HTTP DATAGRAM Capsule path.
    let mut proxy_params = ConnectionParameters::default().datagram_size(0);
    if let Some(v) = proxy_max_stream_data {
        proxy_params = proxy_params.max_stream_data(StreamType::BiDi, true, v);
    }
    // `max_stream_data(BiDi, false, _)` is the window the client grants on the
    // streams it opens, so it bounds what the proxy can send on the CONNECT
    // stream.
    let mut client_params = ConnectionParameters::default().datagram_size(0);
    if let Some(v) = client_max_stream_data {
        client_params = client_params.max_stream_data(StreamType::BiDi, false, v);
    }
    let mut client = http3_client_with_params(
        Http3Parameters::default()
            .connect(true)
            .connection_parameters(client_params),
    );
    let mut proxy = http3_server_with_params(
        Http3Parameters::default()
            .connect(true)
            .connection_parameters(proxy_params),
    );

    let out = test_fixture::connect_peers(&mut client, &mut proxy);
    if let Some(dgram) = out
        && let Some(dgram) = proxy.process(Some(dgram), now()).dgram()
    {
        client.process_input(dgram, now());
    }

    let session_id = client
        .connect_udp_create_session(
            now(),
            &format!("https://[{}]:{}/", DEFAULT_ADDR.ip(), DEFAULT_ADDR.port())
                .parse::<Uri>()
                .unwrap(),
            &[],
        )
        .unwrap();

    exchange_packets(&mut client, &mut proxy, false, None);

    let proxy_session = proxy
        .events()
        .find_map(|event| {
            if let Http3ServerEvent::ConnectUdp(ServerEvent::NewSession { session, headers }) =
                event
            {
                assert_eq!(session.stream_id(), session_id);
                assert!(
                    headers.contains_header(":method", "CONNECT")
                        && headers.contains_header(":protocol", "connect-udp")
                        && headers.contains_header("capsule-protocol", "?1")
                );
                session
                    .response(&SessionAcceptAction::Accept, now())
                    .unwrap();
                Some(session)
            } else {
                None
            }
        })
        .unwrap();

    exchange_packets(&mut client, &mut proxy, false, None);

    client
        .events()
        .find(|e| {
            matches!(
                e,
                Http3ClientEvent::ConnectUdp(ConnectUdpEvent::NewSession { stream_id, status, .. })
                    if *stream_id == session_id && *status == 200)
        })
        .unwrap();

    (client, proxy, session_id, proxy_session)
}

/// A datagram capsule that would exceed the control stream's flow-control window
/// is refused with `FlowControlLimit` rather than silently dropped, and the
/// sender receives a resume event once the window reopens.
#[test]
fn datagram_capsule_flow_control_error_and_resume() {
    let (mut client, mut proxy, session_id, _proxy_session) =
        establish_capsule_session(Some(2000), None);

    // Fill the control stream's flow-control window with datagram capsules until
    // one is refused; a refusal is an error, never a silent drop.
    let payload = vec![0x2c; 500];
    let mut refused = false;
    for _ in 0..100 {
        match client.connect_udp_send_datagram(session_id, &payload, None, now()) {
            Ok(_) => {}
            Err(Error::FlowControlLimit) => {
                refused = true;
                break;
            }
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    assert!(
        refused,
        "the control stream never reached its flow-control limit"
    );
    assert!(
        !client
            .events()
            .any(|e| matches!(e, Http3ClientEvent::OutgoingDatagramSpaceAvailable)),
        "resume event fired before the window reopened"
    );

    // The proxy reads the buffered capsules and grants more flow-control credit,
    // reopening the control stream, which must surface the resume event.
    exchange_packets(&mut client, &mut proxy, false, None);
    assert!(
        client
            .events()
            .any(|e| matches!(e, Http3ClientEvent::OutgoingDatagramSpaceAvailable)),
        "resume event not emitted after control-stream flow control reopened"
    );
}

/// `write_datagram_capsule` checks the control stream's send space against the
/// datagram payload alone, but then writes the payload wrapped in a capsule
/// header and a DATA frame header. With exactly the payload's worth of space
/// left the send is accepted, yet only its head fits the window; the tail stays
/// in the HTTP/3 send buffer, and nothing flushes it because the session is not
/// registered as having pending data. The proxy never sees the datagram.
#[test]
fn datagram_capsule_accepted_without_room_is_stranded() {
    const WINDOW: u64 = 3000;
    let (mut client, mut proxy, session_id, _proxy_session) =
        establish_capsule_session(Some(WINDOW), None);

    // A refused capsule consumes no window, so walk the payload size down until
    // one is accepted. Refusing `p + 1` while accepting `p` means the window
    // holds exactly the payload plus its context ID, and not a byte more.
    let mut payload_len = usize::try_from(WINDOW).unwrap();
    loop {
        let payload = vec![0x2c; payload_len];
        match client.connect_udp_send_datagram(session_id, &payload, None, now()) {
            Ok(true) => break,
            Err(Error::FlowControlLimit) => payload_len -= 1,
            other => panic!("unexpected result: {other:?}"),
        }
    }

    let count = |proxy: &mut Http3Server| {
        proxy
            .events()
            .filter(|e| {
                matches!(
                    e,
                    Http3ServerEvent::ConnectUdp(ServerEvent::Datagram { .. })
                )
            })
            .count()
    };
    let max_stream_data_before = client.transport_stats().frame_rx.max_stream_data;
    exchange_packets(&mut client, &mut proxy, false, None);
    let received = count(&mut proxy);
    // The proxy read the capsule's head and granted more credit, so the tail
    // could have been flushed by now.
    assert!(client.transport_stats().frame_rx.max_stream_data > max_stream_data_before);

    // Only the next capsule pushes the stranded tail out.
    assert_eq!(
        client.connect_udp_send_datagram(session_id, PING, None, now()),
        Ok(true)
    );
    exchange_packets(&mut client, &mut proxy, false, None);
    let received_after_next = count(&mut proxy);

    assert_eq!(
        (received, received_after_next),
        (1, 2),
        "datagram accepted with Ok(true) did not reach the proxy until the next capsule flushed it"
    );
}

/// The proxy sends datagram capsules too. When the CONNECT stream's
/// flow-control window runs out, the server side must behave like the client
/// side: refuse with `FlowControlLimit` rather than drop, and emit
/// [`Http3ServerEvent::OutgoingDatagramSpaceAvailable`] once the window reopens.
#[test]
fn server_datagram_capsule_flow_control_error_and_resume() {
    let (mut client, mut proxy, _session_id, proxy_session) =
        establish_capsule_session(None, Some(2000));

    let payload = vec![0x2c; 500];
    let mut refused = false;
    for _ in 0..100 {
        match proxy_session.send_datagram(&payload, None, now()) {
            Ok(_) => {}
            Err(Error::FlowControlLimit) => {
                refused = true;
                break;
            }
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    assert!(
        refused,
        "the CONNECT stream never reached its flow-control limit"
    );
    assert!(
        !proxy
            .events()
            .any(|e| matches!(e, Http3ServerEvent::OutgoingDatagramSpaceAvailable { .. })),
        "resume event fired before the window reopened"
    );

    // The client reads the buffered capsules and grants more credit, which must
    // release the blocked proxy.
    exchange_packets(&mut client, &mut proxy, false, None);
    assert!(
        proxy
            .events()
            .any(|e| matches!(e, Http3ServerEvent::OutgoingDatagramSpaceAvailable { .. })),
        "resume event not emitted after the CONNECT stream reopened"
    );
}
