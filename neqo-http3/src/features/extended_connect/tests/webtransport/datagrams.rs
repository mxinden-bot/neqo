// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::{num::NonZeroUsize, time::Duration};

use neqo_common::{Encoder, event::Provider as _, to_u64};
use neqo_transport::{
    ConnectionParameters, DatagramQueueOutcome, Output, StreamId, streams::SendGroupId,
};
use test_fixture::now;

use crate::{
    Http3ClientEvent, Http3ServerEvent, WebTransportEvent,
    features::extended_connect::{
        DatagramOutcome,
        tests::webtransport::{DATAGRAM_SIZE, WtTest, wt_default_parameters},
    },
    webtransport::{ClientSession as _, ServerEvent, ServerSession},
};

fn server_datagram_outcomes(wt: &WtTest, session_id: StreamId) -> Vec<DatagramOutcome> {
    wt.server
        .events()
        .filter_map(|e| match e {
            Http3ServerEvent::WebTransport(ServerEvent::DatagramOutcome { session, outcome })
                if session.stream_id() == session_id =>
            {
                Some(outcome)
            }
            _ => None,
        })
        .collect()
}

const DGRAM: &[u8] = &[0, 100];

fn do_datagram_test(wt: &mut WtTest, wt_session: &ServerSession) {
    assert_eq!(
        wt_session.max_datagram_size(),
        Ok(DATAGRAM_SIZE - to_u64(Encoder::varint_len(wt_session.stream_id().as_u64())))
    );
    assert_eq!(
        wt.max_datagram_size(wt_session.stream_id()),
        Ok(DATAGRAM_SIZE - to_u64(Encoder::varint_len(wt_session.stream_id().as_u64())))
    );

    assert_eq!(
        wt_session.send_datagram(DGRAM, None, now(), SendGroupId::new(0), 0),
        Ok(DatagramQueueOutcome::Ok)
    );
    assert_eq!(wt.send_datagram(wt_session.stream_id(), DGRAM), Ok(()));

    wt.exchange_packets();
    wt.check_datagram_received_client(wt_session.stream_id(), DGRAM);
    wt.check_datagram_received_server(wt_session, DGRAM);
}

#[test]
fn datagrams() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    do_datagram_test(&mut wt, &wt_session);
}

#[test]
fn datagrams_multiple_session() {
    let mut wt = WtTest::new();

    let wt_session1 = wt.create_wt_session();
    do_datagram_test(&mut wt, &wt_session1);

    let wt_session_2 = wt.create_wt_session();
    do_datagram_test(&mut wt, &wt_session_2);
}

// A peer is allowed to advertise a max_datagram_frame_size smaller than the
// per-datagram quarter-stream-id prefix. Once a session lands on a stream id
// whose quarter stream id needs a longer varint than the available datagram
// size (quarter stream id >= 64, i.e. stream id >= 256, needs two bytes), the
// prefix subtraction must clamp to zero instead of wrapping.
#[test]
fn max_datagram_size_smaller_than_session_prefix() {
    let params = || {
        wt_default_parameters()
            .connection_parameters(ConnectionParameters::default().datagram_size(1))
    };
    let mut wt = WtTest::new_with_params(params(), params());

    let mut wt_session = wt.create_wt_session();
    while wt_session.stream_id().as_u64() < 256 {
        wt_session = wt.create_wt_session();
    }
    assert_eq!(Encoder::varint_len(wt_session.stream_id().as_u64() >> 2), 2);

    assert_eq!(wt_session.max_datagram_size(), Ok(0));
    assert_eq!(wt.max_datagram_size(wt_session.stream_id()), Ok(0));
}

#[test]
fn datagram_expires_before_being_sent() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let t0 = now();

    wt_session
        .set_datagram_max_age(Some(Duration::from_millis(5)), t0)
        .unwrap();
    assert_eq!(
        wt_session.send_datagram(DGRAM, Some(1), t0, SendGroupId::new(0), 0),
        Ok(DatagramQueueOutcome::Ok)
    );
    assert_eq!(wt_session.datagram_queue_capacity().queued_datagrams, 1);

    // No packets ever need to be built in between: expiry must not wait on
    // that. Driving the server's own HTTP/3 tick (not exchange_packets,
    // which uses its own clock) is enough on its own.
    let later = t0 + Duration::from_millis(10);
    drop(wt.server.process_output(later));

    assert_eq!(
        wt_session.datagram_queue_capacity().queued_datagrams,
        0,
        "the stale datagram must be gone before it is ever handed to the QUIC layer"
    );
    assert_eq!(wt_session.stats().datagrams_expired_outgoing, 1);
}

#[test]
fn datagram_larger_than_peers_limit_is_rejected_synchronously() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();

    let max = wt_session
        .max_datagram_size()
        .expect("datagrams are enabled by default");
    let oversized = vec![0; usize::try_from(max).unwrap() + 1];

    assert_eq!(
        wt_session.send_datagram(&oversized, Some(1), now(), SendGroupId::new(0), 0),
        Err(crate::Error::Transport(neqo_transport::Error::TooMuchData)),
        "an oversized datagram must fail before ever reaching the queue"
    );
    assert_eq!(wt_session.datagram_queue_capacity().queued_datagrams, 0);
}

/// `SessionStats::datagrams_sent_outgoing` must count a datagram sent
/// without a tracking id -- an aggregate delta covering untracked sends is
/// the whole point of counting it here instead of only via per-datagram
/// `DatagramOutcome` events.
#[test]
fn untracked_datagram_sent_is_counted_in_aggregate_stats() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let session_id = wt_session.stream_id();

    assert_eq!(
        wt.client
            .webtransport_send_datagram(session_id, DGRAM, None, now(), SendGroupId::new(0), 0)
            .unwrap(),
        DatagramQueueOutcome::Ok
    );
    wt.exchange_packets();

    let stats = wt.client.webtransport_session_stats(session_id).unwrap();
    assert_eq!(stats.datagrams_sent_outgoing, 1);
    assert_eq!(stats.datagrams_dropped_outgoing, 0);
}

/// `SessionStats::datagrams_dropped_outgoing` must count a datagram evicted
/// from the queue to make room under the byte budget, even without a
/// tracking id.
#[test]
fn untracked_datagram_eviction_is_counted_in_aggregate_stats() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let session_id = wt_session.stream_id();

    for sent in 0.. {
        let outcome = wt
            .client
            .webtransport_send_datagram(session_id, DGRAM, None, now(), SendGroupId::new(0), 0)
            .unwrap();
        if matches!(outcome, DatagramQueueOutcome::Overflowed { .. }) {
            break;
        }
        assert!(sent < 1_000_000, "byte budget should have been hit by now");
    }

    let stats = wt.client.webtransport_session_stats(session_id).unwrap();
    assert_eq!(stats.datagrams_dropped_outgoing, 1);
}

/// [`Http3Client::webtransport_datagram_queue_capacity`] must reflect this
/// session's own byte-budgeted queue - the one a content-process credit
/// grant should track - and not be bounded by the legacy count-limited FIFO.
/// Both live in `neqo-transport`: `send_datagram` hands the datagram
/// straight to the per-session queue, so nothing is ever held at the HTTP/3
/// layer.
#[test]
fn datagram_queue_capacity_reflects_the_session_queue_not_the_legacy_fifo() {
    // A burst larger than the 10-slot legacy FIFO must still be reflected
    // faithfully by the per-session queue's own count.
    const BURST: u8 = 20;

    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let session_id = wt_session.stream_id();

    let before = wt
        .client
        .webtransport_datagram_queue_capacity(session_id)
        .unwrap();
    assert_eq!(before.queued_datagrams, 0);

    for i in 0..BURST {
        _ = wt
            .client
            .webtransport_send_datagram(
                session_id,
                &[0, i],
                Some(u64::from(i)),
                now(),
                SendGroupId::new(0),
                0,
            )
            .unwrap();
    }

    let after = wt
        .client
        .webtransport_datagram_queue_capacity(session_id)
        .unwrap();
    assert_eq!(after.queued_datagrams, usize::from(BURST));
    assert!(
        after.remaining_bytes < before.remaining_bytes,
        "enqueuing datagrams must consume some of the byte budget"
    );
}

#[test]
fn datagram_high_water_mark_signals_backpressure() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let t0 = now();

    wt_session
        .set_datagram_high_water_mark(Some(NonZeroUsize::new(2).unwrap()))
        .unwrap();
    assert_eq!(
        wt_session.send_datagram(DGRAM, Some(1), t0, SendGroupId::new(0), 0),
        Ok(DatagramQueueOutcome::Ok)
    );
    assert_eq!(
        wt_session.send_datagram(DGRAM, Some(2), t0, SendGroupId::new(0), 0),
        Ok(DatagramQueueOutcome::AboveWatermark),
        "the second datagram crosses the high water mark"
    );
}

/// The queue's resume signal must still reach the HTTP/3 server's event
/// queue, which nothing else covers any more:
/// `datagram_high_water_mark_signals_backpressure` stops at the transport
/// outcome, and connect-udp has no way to set a high water mark to drive
/// this from.
#[test]
fn outgoing_datagram_space_available_forwarded() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let t0 = now();

    wt_session
        .set_datagram_high_water_mark(Some(NonZeroUsize::new(1).unwrap()))
        .unwrap();
    assert_eq!(
        wt_session.send_datagram(DGRAM, Some(1), t0, SendGroupId::new(0), 0),
        Ok(DatagramQueueOutcome::AboveWatermark)
    );
    assert!(
        !wt.server
            .events()
            .any(|e| matches!(e, Http3ServerEvent::OutgoingDatagramSpaceAvailable { .. })),
        "server resume event fired before the queue drained"
    );

    wt.exchange_packets();

    assert!(
        wt.server
            .events()
            .any(|e| matches!(e, Http3ServerEvent::OutgoingDatagramSpaceAvailable { .. })),
        "OutgoingDatagramSpaceAvailable was not forwarded to the HTTP/3 server"
    );
}

#[test]
fn server_processes_a_connection_whose_only_pending_work_is_an_expired_datagram() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let t0 = now();

    wt_session
        .set_datagram_max_age(Some(Duration::from_millis(5)), t0)
        .unwrap();
    // set_datagram_max_age marks the connection as needing processing on its
    // own; flush that here so it can't mask the check below. From this point
    // on, only the datagram's own expiry may give the connection a reason to
    // be processed.
    drop(wt.server.process_output(t0));
    // Untracked (id: None): the queue reports no per-datagram outcome for a
    // send either way, but a tracked one leaves an ID to report on later,
    // and this test wants the datagram's expiry to be the only thing that
    // can give the connection a reason to be processed.
    _ = wt_session
        .send_datagram_without_marking_needs_processing(DGRAM, None, t0)
        .unwrap();
    assert_eq!(wt_session.datagram_queue_capacity().queued_datagrams, 1);

    let later = t0 + Duration::from_millis(10);
    drop(wt.server.process_output(later));

    assert_eq!(
        wt_session.datagram_queue_capacity().queued_datagrams,
        0,
        "the datagram's own expiry must get this connection processed"
    );
    assert_eq!(wt_session.stats().datagrams_expired_outgoing, 1);
}

#[test]
fn datagram_send_order_controls_priority() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let t0 = now();

    // Enqueue the low-priority one first; both ungrouped (SendGroupId::new(0)),
    // so send_order alone must decide delivery order.
    assert_eq!(
        wt_session.send_datagram(b"low", Some(1), t0, SendGroupId::new(0), 1),
        Ok(DatagramQueueOutcome::Ok)
    );
    assert_eq!(
        wt_session.send_datagram(b"high", Some(2), t0, SendGroupId::new(0), 10),
        Ok(DatagramQueueOutcome::Ok)
    );

    wt.exchange_packets();

    let received: Vec<Vec<u8>> = wt
        .client
        .events()
        .filter_map(|e| match e {
            Http3ClientEvent::WebTransport(WebTransportEvent::Datagram { datagram, .. }) => {
                Some(datagram.as_ref().to_vec())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        received,
        vec![b"high".to_vec(), b"low".to_vec()],
        "the higher send_order datagram must be delivered first"
    );
}

#[test]
fn datagram_send_updates_sent_stat() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let t0 = now();

    assert_eq!(
        wt_session.send_datagram(DGRAM, None, t0, SendGroupId::new(0), 0),
        Ok(DatagramQueueOutcome::Ok)
    );
    assert_eq!(wt_session.stats().datagrams_sent_outgoing, 0);

    wt.exchange_packets();

    assert_eq!(wt_session.stats().datagrams_sent_outgoing, 1);
}

#[test]
fn datagram_expiry_reports_expired_outcome() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let t0 = now();

    wt_session
        .set_datagram_max_age(Some(Duration::from_millis(5)), t0)
        .unwrap();
    _ = wt_session
        .send_datagram(DGRAM, Some(9), t0, SendGroupId::new(0), 0)
        .unwrap();

    drop(wt.server.process_output(t0 + Duration::from_millis(10)));

    assert_eq!(
        server_datagram_outcomes(&wt, wt_session.stream_id()),
        vec![DatagramOutcome::Expired(9)]
    );
    assert_eq!(wt_session.stats().datagrams_sent_outgoing, 0);
}

#[test]
fn session_close_reports_dropped_outcome_for_queued_datagrams() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let t0 = now();

    _ = wt_session
        .send_datagram(DGRAM, Some(3), t0, SendGroupId::new(0), 0)
        .unwrap();
    let stats = wt_session.close_session(0, "bye", t0).unwrap();
    drop(wt.server.process_output(t0));

    assert_eq!(
        server_datagram_outcomes(&wt, wt_session.stream_id()),
        vec![DatagramOutcome::Dropped(3)],
        "close_session must drop and report any datagram still queued"
    );
    assert_eq!(
        stats.datagrams_dropped_outgoing, 1,
        "the stats close_session returns must already count that drop"
    );
}

#[test]
fn datagram_expires_on_the_implementation_defined_default_max_age() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let t0 = now();

    // No set_datagram_max_age call: outgoingMaxAge is left at its default.
    _ = wt_session
        .send_datagram(DGRAM, Some(13), t0, SendGroupId::new(0), 0)
        .unwrap();

    // Comfortably past the default, so this expires on the very first
    // drain rather than getting sent.
    drop(wt.server.process_output(t0 + Duration::from_secs(1)));

    assert_eq!(
        server_datagram_outcomes(&wt, wt_session.stream_id()),
        vec![DatagramOutcome::Expired(13)]
    );
}

/// A session reset by the peer is removed outright, and its outgoing datagram
/// queue lives on `Connection`, so it outlives the session unless teardown
/// drops it. The transport's own timer would still expire what is on it, but
/// silently: no session is left to report a `Dropped`/`Expired` outcome or
/// count it, and the queue entry sticks around until the connection ends.
#[test]
fn session_reset_by_peer_drops_queued_datagrams() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let session_id = wt_session.stream_id();
    let t0 = now();

    // Many packets' worth: a single `process_output` builds one packet, so most
    // of these are still queued whichever way the sweep and the packet build
    // interleave. A handful of small datagrams would all fit in one packet and
    // the test would pass vacuously.
    let big = vec![0x5a; 1000];
    for id in 0..20 {
        assert_eq!(
            wt.client.webtransport_send_datagram(
                session_id,
                &big,
                Some(id),
                t0,
                SendGroupId::new(0),
                0
            ),
            Ok(DatagramQueueOutcome::Ok)
        );
    }

    // Deliver the server's reset without letting the client send: going
    // through `WtTest::cancel_session_server` would exchange packets and flush
    // the queue before the session ever closes.
    wt_session
        .cancel_fetch(crate::Error::HttpNone.code())
        .unwrap();
    let mut t = t0;
    let reset = loop {
        match wt.server.process_output(t) {
            Output::Datagram(d) => break d,
            Output::Callback(delay) => t += delay,
            Output::None => t += Duration::from_millis(1),
        }
        assert!(t < t0 + Duration::from_millis(50), "server sent no reset");
    };
    wt.client.process_input(reset, t);
    drop(wt.client.process_output(t));

    let outcomes = client_datagram_outcomes(&mut wt, session_id);
    assert!(!outcomes.is_empty(), "expected datagrams to be left queued");
    assert!(
        outcomes
            .iter()
            .all(|o| matches!(o, DatagramOutcome::Dropped(_))),
        "every datagram left queued at close must be reported dropped, got {outcomes:?}"
    );
    assert_eq!(
        wt.client.connection().next_datagram_expiry(),
        None,
        "nothing may stay queued for a closed session, or its expiry keeps coming due"
    );
}

fn client_datagram_outcomes(wt: &mut WtTest, session_id: StreamId) -> Vec<DatagramOutcome> {
    wt.client
        .events()
        .filter_map(|e| match e {
            Http3ClientEvent::WebTransport(WebTransportEvent::DatagramOutcome {
                session_id: sid,
                outcome,
            }) if sid == session_id => Some(outcome),
            _ => None,
        })
        .collect()
}

#[test]
fn client_set_datagram_max_age_reports_expired_outcome() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let session_id = wt_session.stream_id();
    let t0 = now();

    assert_eq!(
        wt.client.webtransport_send_datagram(
            session_id,
            DGRAM,
            Some(7),
            t0,
            SendGroupId::new(0),
            0
        ),
        Ok(DatagramQueueOutcome::Ok)
    );

    let t1 = t0 + Duration::from_millis(200);
    wt.client
        .webtransport_set_datagram_max_age(session_id, Some(Duration::from_millis(100)), t1)
        .unwrap();

    assert_eq!(
        client_datagram_outcomes(&mut wt, session_id),
        vec![DatagramOutcome::Expired(7)],
        "shortening max_age past an already-queued datagram must expire it immediately"
    );
}

/// A burst exceeding the byte budget, with a mix of send-order priorities,
/// must evict low-priority datagrams to make room for high-priority ones -
/// verified through the real `Http3Client` API and a live connection, not
/// just on a bare `DatagramQueue` in isolation. Each datagram's payload is
/// its own id (as 8 little-endian bytes), so delivery can be checked by
/// content rather than relying on a per-datagram "sent" outcome, which the
/// queue deliberately does not report (see `DatagramOutcome`).
#[test]
fn datagram_burst_exceeding_byte_budget_preserves_priority_through_a_live_connection() {
    const HIGH_PRIORITY_COUNT: usize = 5;

    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let session_id = wt_session.stream_id();

    // Fill the byte budget with low-priority (order=0) datagrams, without
    // draining, until eviction starts - i.e. until the budget is full.
    let mut low_priority_ids = Vec::new();
    let mut next_id: u64 = 0;
    loop {
        let outcome = wt
            .client
            .webtransport_send_datagram(
                session_id,
                &next_id.to_le_bytes(),
                Some(next_id),
                now(),
                SendGroupId::new(0),
                0,
            )
            .unwrap();
        if let DatagramQueueOutcome::Overflowed { dropped, .. } = outcome {
            // enqueue() always accepts: this datagram itself got in,
            // evicting `dropped` (the oldest) to make room for it.
            for id in dropped {
                low_priority_ids.retain(|&x| x != id);
            }
            low_priority_ids.push(next_id);
            next_id += 1;
            break;
        }
        assert_eq!(
            outcome,
            DatagramQueueOutcome::Ok,
            "unexpected outcome before the byte budget is hit"
        );
        low_priority_ids.push(next_id);
        next_id += 1;
        assert!(
            next_id < 1_000_000,
            "byte budget should have been hit by now"
        );
    }

    // Now send a few high-priority datagrams.
    let high_priority_ids: Vec<u64> = (0..to_u64(HIGH_PRIORITY_COUNT))
        .map(|i| next_id + i)
        .collect();
    let mut evicted = Vec::new();
    for &id in &high_priority_ids {
        let outcome = wt
            .client
            .webtransport_send_datagram(
                session_id,
                &id.to_le_bytes(),
                Some(id),
                now(),
                SendGroupId::new(0),
                10,
            )
            .unwrap();
        match outcome {
            DatagramQueueOutcome::Overflowed { dropped, .. } => evicted.extend(dropped),
            other => panic!("expected an eviction for the high-priority datagram: {other:?}"),
        }
    }
    assert_eq!(
        evicted,
        low_priority_ids[..HIGH_PRIORITY_COUNT],
        "eviction must take the oldest low-priority datagrams first, never the high-priority ones"
    );

    wt.exchange_packets();

    let received: Vec<_> = wt
        .server
        .events()
        .filter_map(|e| match e {
            Http3ServerEvent::WebTransport(ServerEvent::Datagram { session, datagram })
                if session.stream_id() == session_id =>
            {
                Some(datagram)
            }
            _ => None,
        })
        .collect();
    let was_received = |id: u64| received.iter().any(|d| d.as_ref() == id.to_le_bytes());

    for &id in &high_priority_ids {
        assert!(was_received(id), "high-priority datagram {id} must be sent");
    }
    for &id in &evicted {
        assert!(
            !was_received(id),
            "evicted low-priority datagram {id} must not be sent"
        );
    }
}

#[test]
fn client_set_datagram_high_water_mark_signals_backpressure() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let session_id = wt_session.stream_id();
    let t0 = now();

    wt.client
        .webtransport_set_datagram_high_water_mark(session_id, Some(NonZeroUsize::new(2).unwrap()))
        .unwrap();

    assert_eq!(
        wt.client.webtransport_send_datagram(
            session_id,
            DGRAM,
            Some(1),
            t0,
            SendGroupId::new(0),
            0
        ),
        Ok(DatagramQueueOutcome::Ok)
    );
    assert_eq!(
        wt.client.webtransport_send_datagram(
            session_id,
            DGRAM,
            Some(2),
            t0,
            SendGroupId::new(0),
            0
        ),
        Ok(DatagramQueueOutcome::AboveWatermark),
        "the second datagram crosses the high water mark"
    );
}

// ── Review: conservation of outgoing datagrams ─────────────────────────────

/// Deterministic xorshift, so a failure is reproducible from its seed.
struct Rng(u64);

impl Rng {
    const fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    const fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Pump both ends until quiet, carrying a monotonic clock so max-age expiry
/// actually fires. `WtTest::exchange_packets` restarts from `now()` on every
/// call, which freezes time and hides anything age-related.
fn exchange_at(wt: &mut WtTest, clock: &mut std::time::Instant) {
    const RTT: Duration = Duration::from_millis(10);
    let mut out = None;
    for _ in 0..100 {
        *clock += RTT / 2;
        out = wt.client.process(out, *clock).dgram();
        let client_quiet = out.is_none();
        *clock += RTT / 2;
        out = wt.server.process(out, *clock).dgram();
        if client_quiet && out.is_none() {
            return;
        }
    }
    panic!("exchange did not settle");
}

/// Every datagram the queue accepts must end up in exactly one of the three
/// per-session counters: sent, expired, or dropped. Nothing may vanish, and
/// nothing may be counted twice.
///
/// Randomised over sizes, send groups, send orders and inter-burst delays, so
/// it exercises eviction, rejection and max-age expiry together rather than
/// one at a time.
#[test]
fn outgoing_datagram_accounting_is_conserved() {
    for seed in 1..8_u64 {
        let mut rng = Rng(seed);
        let mut wt = WtTest::new();
        let wt_session = wt.create_wt_session();
        let session_id = wt_session.stream_id();
        let mut clock = now();
        let mut accepted = 0_u64;
        let mut next_id = 0_u64;
        let groups: Vec<SendGroupId> = std::iter::once(SendGroupId::new(0))
            .chain(std::iter::repeat_with(|| {
                wt.client
                    .webtransport_create_send_group(session_id)
                    .expect("create send group")
            }))
            .take(3)
            .collect();

        for _ in 0..12 {
            for _ in 0..25 {
                next_id += 1;
                let len = 1 + usize::try_from(rng.below(900)).expect("small");
                let outcome = wt
                    .client
                    .webtransport_send_datagram(
                        session_id,
                        &vec![7; len],
                        Some(next_id),
                        clock,
                        groups[usize::try_from(rng.below(3)).expect("small")],
                        i64::try_from(rng.below(3)).expect("small"),
                    )
                    .unwrap_or_else(|e| {
                        panic!("seed {seed}: send_datagram failed with {e:?} (len={len})")
                    });
                drop(outcome);
                accepted += 1;
            }
            // Change outgoingMaxAge mid-flight, which expires queued
            // datagrams synchronously and must still count them.
            if rng.below(3) == 0 {
                wt.client
                    .webtransport_set_datagram_max_age(
                        session_id,
                        Some(Duration::from_millis(1 + rng.below(80))),
                        clock,
                    )
                    .unwrap();
            }
            clock += Duration::from_millis(rng.below(60));
            exchange_at(&mut wt, &mut clock);
        }

        // Let anything still queued expire or drain.
        clock += Duration::from_secs(2);
        exchange_at(&mut wt, &mut clock);

        let received = wt
            .server
            .events()
            .filter(|e| {
                matches!(
                    e,
                    Http3ServerEvent::WebTransport(ServerEvent::Datagram { .. })
                )
            })
            .count();

        let stats = wt.client.webtransport_session_stats(session_id).unwrap();
        assert_eq!(
            stats.datagrams_sent_outgoing
                + stats.datagrams_expired_outgoing
                + stats.datagrams_dropped_outgoing,
            accepted,
            "seed {seed}: datagrams went missing. sent={} expired={} dropped={} accepted={accepted}",
            stats.datagrams_sent_outgoing,
            stats.datagrams_expired_outgoing,
            stats.datagrams_dropped_outgoing,
        );
        assert_eq!(
            to_u64(received),
            stats.datagrams_sent_outgoing,
            "seed {seed}: the peer did not receive every datagram counted as sent"
        );
        assert_eq!(
            wt_session.datagram_queue_capacity().queued_datagrams,
            0,
            "seed {seed}: datagrams left queued after everything settled"
        );
    }
}

/// Two sessions on one connection, hammered concurrently. Each session's
/// three counters must add up to what that session accepted, with no
/// cross-session leakage: a connection-wide expiry sweep that handed
/// whichever session swept first every other session's expired IDs would
/// show up here as one session over-counting and the other under-counting.
#[test]
fn per_session_datagram_accounting_does_not_leak_across_sessions() {
    for seed in 1..6_u64 {
        let mut rng = Rng(seed);
        let mut wt = WtTest::new();
        let session_a = wt.create_wt_session().stream_id();
        let session_b = wt.create_second_wt_session();
        let mut clock = now();
        let mut accepted = [0_u64, 0];
        let mut next_id = 0_u64;

        for _ in 0..10 {
            for _ in 0..20 {
                next_id += 1;
                let which = usize::try_from(rng.below(2)).expect("small");
                let session = if which == 0 { session_a } else { session_b };
                let len = 1 + usize::try_from(rng.below(900)).expect("small");
                _ = wt
                    .client
                    .webtransport_send_datagram(
                        session,
                        &vec![7; len],
                        Some(next_id),
                        clock,
                        SendGroupId::new(0),
                        i64::try_from(rng.below(3)).expect("small"),
                    )
                    .unwrap_or_else(|e| panic!("seed {seed}: send_datagram failed with {e:?}"));
                accepted[which] += 1;
            }
            clock += Duration::from_millis(rng.below(60));
            exchange_at(&mut wt, &mut clock);
        }

        clock += Duration::from_secs(2);
        exchange_at(&mut wt, &mut clock);

        for (which, session) in [session_a, session_b].into_iter().enumerate() {
            let stats = wt.client.webtransport_session_stats(session).unwrap();
            assert_eq!(
                stats.datagrams_sent_outgoing
                    + stats.datagrams_expired_outgoing
                    + stats.datagrams_dropped_outgoing,
                accepted[which],
                "seed {seed}: session {which} accounting drifted. sent={} expired={} dropped={} accepted={}",
                stats.datagrams_sent_outgoing,
                stats.datagrams_expired_outgoing,
                stats.datagrams_dropped_outgoing,
                accepted[which],
            );
        }
    }
}

/// `DatagramQueueOutcome::Rejected` promises "Nothing else was disturbed":
/// the incoming datagram is refused outright rather than evicting something
/// that outranks it. Check that literally, through the public API.
#[test]
fn a_rejected_datagram_disturbs_nothing() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let session_id = wt_session.stream_id();
    let t0 = now();

    // Fill the byte budget with high-priority datagrams.
    let mut queued = 0_u64;
    loop {
        queued += 1;
        let outcome = wt
            .client
            .webtransport_send_datagram(
                session_id,
                &[9; 512],
                Some(queued),
                t0,
                SendGroupId::new(0),
                10,
            )
            .unwrap();
        if !matches!(outcome, DatagramQueueOutcome::Ok) {
            break;
        }
        assert!(queued < 1_000_000, "byte budget should have been hit");
    }
    let before = wt_session.datagram_queue_capacity();

    // A lower-priority newcomer must be refused, leaving the queue untouched.
    assert_eq!(
        wt.client
            .webtransport_send_datagram(
                session_id,
                &[1; 512],
                Some(u64::MAX),
                t0,
                SendGroupId::new(0),
                0,
            )
            .unwrap(),
        DatagramQueueOutcome::Rejected
    );
    assert_eq!(
        wt_session.datagram_queue_capacity(),
        before,
        "a rejected datagram must leave the queue exactly as it was"
    );
}

/// Backpressure must always lift. Model an application that follows the
/// contract literally: it sends until `send_datagram` reports anything but
/// `Ok`, then stops until it sees `OutgoingDatagramSpaceAvailable`.
///
/// The failure this is looking for is a lost resume signal: once the queue
/// has drained, a sender still waiting has stalled for good, because nothing
/// else will ever revisit that queue on its behalf.
#[test]
fn a_backpressured_sender_is_always_resumed() {
    for seed in 1..10_u64 {
        let mut rng = Rng(seed);
        let mut wt = WtTest::new();
        let wt_session = wt.create_wt_session();
        let session_id = wt_session.stream_id();
        let mut clock = now();
        let mut next_id = 0_u64;
        let mut blocked = false;

        // A mark low enough that backpressure is reached constantly.
        wt.client
            .webtransport_set_datagram_high_water_mark(session_id, NonZeroUsize::new(2))
            .unwrap();

        for round in 0..40 {
            if !blocked {
                for _ in 0..=rng.below(4) {
                    next_id += 1;
                    let outcome = wt
                        .client
                        .webtransport_send_datagram(
                            session_id,
                            &[3; 64],
                            Some(next_id),
                            clock,
                            SendGroupId::new(0),
                            0,
                        )
                        .unwrap();
                    if outcome != DatagramQueueOutcome::Ok {
                        blocked = true;
                        break;
                    }
                }
            }

            clock += Duration::from_millis(rng.below(40));
            exchange_at(&mut wt, &mut clock);

            if wt
                .client
                .events()
                .any(|e| matches!(e, Http3ClientEvent::OutgoingDatagramSpaceAvailable))
            {
                blocked = false;
            }

            assert!(
                !(blocked && wt_session.datagram_queue_capacity().queued_datagrams == 0),
                "seed {seed} round {round}: sender still blocked with an empty queue, \
                 so the resume signal was lost"
            );
        }
    }
}

/// Same liveness model, but the application also changes
/// `outgoingMaxBufferedDatagrams` while it is running, which is what the
/// `WebIDL` attribute lets it do at any time.
///
/// This is the end-to-end face of the missing `resume_if_unblocked` in
/// `QuicDatagrams::set_datagram_high_water_mark`.
#[test]
fn a_backpressured_sender_is_resumed_when_the_mark_is_raised() {
    for seed in 1..10_u64 {
        let mut rng = Rng(seed);
        let mut wt = WtTest::new();
        let wt_session = wt.create_wt_session();
        let session_id = wt_session.stream_id();
        let mut clock = now();
        let mut next_id = 0_u64;
        let mut blocked = false;

        wt.client
            .webtransport_set_datagram_high_water_mark(session_id, NonZeroUsize::new(2))
            .unwrap();

        for round in 0..40 {
            if !blocked {
                for _ in 0..=rng.below(4) {
                    next_id += 1;
                    let outcome = wt
                        .client
                        .webtransport_send_datagram(
                            session_id,
                            &[3; 64],
                            Some(next_id),
                            clock,
                            SendGroupId::new(0),
                            0,
                        )
                        .unwrap();
                    if outcome != DatagramQueueOutcome::Ok {
                        blocked = true;
                        break;
                    }
                }
            }

            if rng.below(3) == 0 {
                wt.client
                    .webtransport_set_datagram_high_water_mark(
                        session_id,
                        NonZeroUsize::new(1 + usize::try_from(rng.below(20)).expect("small")),
                    )
                    .unwrap();
            }

            clock += Duration::from_millis(rng.below(40));
            exchange_at(&mut wt, &mut clock);

            if wt
                .client
                .events()
                .any(|e| matches!(e, Http3ClientEvent::OutgoingDatagramSpaceAvailable))
            {
                blocked = false;
            }

            assert!(
                !(blocked && wt_session.datagram_queue_capacity().queued_datagrams == 0),
                "seed {seed} round {round}: sender still blocked with an empty queue, \
                 so the resume signal was lost"
            );
        }
    }
}

/// The same conservation law, driven from the server, whose processing is
/// gated by `Http3ServerHandler::should_be_processed`. A connection whose
/// only pending work is a datagram expiry has to be selected for a tick, or
/// nothing runs the per-session sweep that counts the outcome, and the
/// counters silently under-report.
#[test]
fn server_side_outgoing_datagram_accounting_is_conserved() {
    for seed in 1..6_u64 {
        let mut rng = Rng(seed);
        let mut wt = WtTest::new();
        let wt_session = wt.create_wt_session();
        let mut clock = now();
        let mut accepted = 0_u64;
        let mut next_id = 0_u64;

        for _ in 0..12 {
            for _ in 0..20 {
                next_id += 1;
                let len = 1 + usize::try_from(rng.below(900)).expect("small");
                _ = wt_session
                    .send_datagram(
                        &vec![5; len],
                        Some(next_id),
                        clock,
                        SendGroupId::new(0),
                        i64::try_from(rng.below(3)).expect("small"),
                    )
                    .unwrap_or_else(|e| panic!("seed {seed}: send_datagram failed with {e:?}"));
                accepted += 1;
            }
            clock += Duration::from_millis(rng.below(60));
            exchange_at(&mut wt, &mut clock);
        }

        clock += Duration::from_secs(2);
        exchange_at(&mut wt, &mut clock);

        let stats = wt_session.stats();
        assert_eq!(
            stats.datagrams_sent_outgoing
                + stats.datagrams_expired_outgoing
                + stats.datagrams_dropped_outgoing,
            accepted,
            "seed {seed}: sent={} expired={} dropped={} accepted={accepted}",
            stats.datagrams_sent_outgoing,
            stats.datagrams_expired_outgoing,
            stats.datagrams_dropped_outgoing,
        );
        assert_eq!(
            wt_session.datagram_queue_capacity().queued_datagrams,
            0,
            "seed {seed}: datagrams left queued after everything settled"
        );
    }
}

/// End-to-end face of the missing expire-before-enqueue: an application that
/// sends twice between two ticks is told to back off because of a datagram
/// that is already past `outgoingMaxAge` and should have been shed.
///
/// `Session::send_datagram` goes straight to `Connection::enqueue_datagram`,
/// and the per-tick sweep only runs inside `process_http3`, so a burst of
/// sends in one task sees a queue that nothing has swept.
#[test]
fn a_stale_datagram_does_not_backpressure_the_next_send() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let session_id = wt_session.stream_id();
    let t0 = now();

    wt.client
        .webtransport_set_datagram_max_age(session_id, Some(Duration::from_millis(5)), t0)
        .unwrap();
    wt.client
        .webtransport_set_datagram_high_water_mark(session_id, NonZeroUsize::new(1))
        .unwrap();

    assert_eq!(
        wt.client
            .webtransport_send_datagram(session_id, DGRAM, Some(1), t0, SendGroupId::new(0), 0)
            .unwrap(),
        DatagramQueueOutcome::AboveWatermark
    );

    // No tick in between: the application just sends again, later.
    let t1 = t0 + Duration::from_millis(50);
    assert_eq!(
        wt.client
            .webtransport_send_datagram(session_id, DGRAM, Some(2), t1, SendGroupId::new(0), 0)
            .unwrap(),
        DatagramQueueOutcome::Ok,
        "datagram 1 is ten times past its max age; it must be shed as expired \
         rather than counted against the high water mark"
    );
}

/// Scheduling and eviction disagree about fairness between send groups.
///
/// `take_next` round-robins across groups, so each gets an equal number of
/// turns. `evict_lowest_priority` instead picks the globally lowest
/// `(send_order, group_id)`, breaking ties by group ID, lowest first. At
/// equal `send_order` that makes the lowest-numbered group the permanent
/// sacrifice, and `SendGroupId::new(0)` is the sentinel for the null
/// sendGroup, so datagrams the application never grouped are the first to
/// go.
///
/// Under sustained overflow, which is what the byte budget exists for, the
/// eviction bias overrides the scheduler's fairness: a group can be starved
/// of the wire despite being served round-robin, because its datagrams are
/// evicted before its turn comes round.
#[test]
fn eviction_does_not_defeat_round_robin_fairness() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let session_id = wt_session.stream_id();
    let mut clock = now();
    let group_b = wt
        .client
        .webtransport_create_send_group(session_id)
        .unwrap();
    let groups = [SendGroupId::new(0), group_b];

    // Bursts far larger than the connection drains, so the byte budget is
    // under constant pressure and eviction runs on nearly every send.
    let mut next_id = 0_u64;
    for _ in 0..8 {
        for _ in 0..400 {
            for (which, group) in groups.into_iter().enumerate() {
                next_id += 1;
                let mut payload = vec![0_u8; 900];
                payload[0] = u8::try_from(which).expect("0 or 1");
                // Same send_order for both: only the group ID differs.
                _ = wt
                    .client
                    .webtransport_send_datagram(
                        session_id,
                        &payload,
                        Some(next_id),
                        clock,
                        group,
                        0,
                    )
                    .unwrap();
            }
        }
        clock += Duration::from_millis(5);
        exchange_at(&mut wt, &mut clock);
    }

    let mut delivered = [0_usize, 0];
    for e in wt.server.events() {
        if let Http3ServerEvent::WebTransport(ServerEvent::Datagram { datagram, .. }) = e {
            delivered[usize::from(datagram.as_ref()[0])] += 1;
        }
    }

    let total = delivered[0] + delivered[1];
    assert!(total > 0, "nothing was delivered");
    assert!(
        delivered[0] * 4 >= total,
        "the null send group got {}/{total} of the wire: eviction by lowest group ID \
         starves it despite round-robin scheduling",
        delivered[0]
    );
}

/// The same bias, between two groups the application created itself, to show
/// it is not specific to the null sendGroup sentinel.
///
/// `send_group::Generator` mints IDs from 1 upwards and never reuses one, so
/// "lowest group ID loses" means the group an application created first is
/// permanently outranked by every group it creates later, at equal
/// `send_order`. An application that mints a group per frame would starve
/// its oldest groups by construction.
#[test]
fn eviction_is_fair_between_two_created_groups() {
    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let session_id = wt_session.stream_id();
    let mut clock = now();
    let first = wt
        .client
        .webtransport_create_send_group(session_id)
        .unwrap();
    let second = wt
        .client
        .webtransport_create_send_group(session_id)
        .unwrap();
    assert!(first < second, "IDs are minted in increasing order");

    let mut next_id = 0_u64;
    for _ in 0..8 {
        for _ in 0..400 {
            for (which, group) in [first, second].into_iter().enumerate() {
                next_id += 1;
                let mut payload = vec![0_u8; 900];
                payload[0] = u8::try_from(which).expect("0 or 1");
                _ = wt
                    .client
                    .webtransport_send_datagram(
                        session_id,
                        &payload,
                        Some(next_id),
                        clock,
                        group,
                        0,
                    )
                    .unwrap();
            }
        }
        clock += Duration::from_millis(5);
        exchange_at(&mut wt, &mut clock);
    }

    let mut delivered = [0_usize, 0];
    for e in wt.server.events() {
        if let Http3ServerEvent::WebTransport(ServerEvent::Datagram { datagram, .. }) = e {
            delivered[usize::from(datagram.as_ref()[0])] += 1;
        }
    }

    let total = delivered[0] + delivered[1];
    assert!(total > 0, "nothing was delivered");
    assert!(
        delivered[0] * 4 >= total,
        "the first-created group got {}/{total} of the wire: at equal send_order, \
         eviction always takes the lower group ID",
        delivered[0]
    );
}

/// Tighter variant of `session_reset_by_peer_drops_queued_datagrams`.
///
/// Differences: the count is pinned rather than asserted non-empty, the
/// server loop never advances the clock by a fixed step, and the peer is
/// checked for the thing the change actually prevents, namely datagrams
/// going out on the wire for a session that no longer exists.
#[test]
fn session_reset_by_peer_drops_queued_datagrams_tighter() {
    const QUEUED: u64 = 20;

    let mut wt = WtTest::new();
    let wt_session = wt.create_wt_session();
    let session_id = wt_session.stream_id();
    let t0 = now();

    for id in 0..QUEUED {
        assert_eq!(
            wt.client.webtransport_send_datagram(
                session_id,
                &[0x5a; 1000],
                Some(id),
                t0,
                SendGroupId::new(0),
                0
            ),
            Ok(DatagramQueueOutcome::Ok)
        );
    }

    wt_session
        .cancel_fetch(crate::Error::HttpNone.code())
        .unwrap();

    // Only the server's own timer advances the clock. `Output::None` means it
    // has nothing to send and no timer armed, so no reset is ever coming: that
    // is the failure, not a reason to poll again a millisecond later.
    let mut t = t0;
    let reset = loop {
        match wt.server.process_output(t) {
            Output::Datagram(d) => break d,
            Output::Callback(delay) => t += delay,
            Output::None => panic!("server has no reset to send and no timer armed"),
        }
    };

    wt.client.process_input(reset, t);
    // Drive to quiescence rather than building a single packet, so teardown
    // cannot be half-done when the assertions run.
    exchange_at(&mut wt, &mut t);

    let outcomes = client_datagram_outcomes(&mut wt, session_id);
    assert_eq!(
        outcomes.len(),
        usize::try_from(QUEUED).expect("small"),
        "every queued datagram must be reported exactly once, got {outcomes:?}"
    );
    assert!(
        outcomes
            .iter()
            .all(|o| matches!(o, DatagramOutcome::Dropped(_))),
        "a datagram queued at close is dropped, not expired or sent: {outcomes:?}"
    );
    assert_eq!(
        wt.server
            .events()
            .filter(|e| matches!(
                e,
                Http3ServerEvent::WebTransport(ServerEvent::Datagram { .. })
            ))
            .count(),
        0,
        "nothing may reach the peer on behalf of a session that is gone"
    );
    assert_eq!(
        wt.client.connection().next_datagram_expiry(),
        None,
        "nothing may stay queued for a closed session, or its expiry keeps coming due"
    );
}
