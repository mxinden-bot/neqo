# quic-rebind-test

A throwaway probe (parked here for easy transfer, unrelated to neqo itself) for
how a QUIC/HTTP3 server reacts when the client's UDP source changes under a live
connection: a source-port change (NAT rebinding) or a source-address change (an
IPv6 privacy-address rotation, or a wifi/wired flip).

A QUIC client cannot swap its socket mid-connection in quic-go, so the change is
done by a tiny in-process UDP relay between the client and the real server that
rebinds its upstream socket. To the server that is exactly a client whose
4-tuple moved.

## Build

    cd qtest && go build -o quic-rebind-test .

A prebuilt linux/amd64 binary is checked in as `quic-rebind-test`.

## The Cloudflare test: send from one IPv6, then another

    ./quic-rebind-test cfflip -a <ipv6A> -b <ipv6B>

Opens one HTTP/3 DoH connection to Cloudflare (cloudflare-dns.com,
2606:4700:4700::1111), issues a DNS query every second, and after `-flip`
(default 6s) flips the upstream source address from A to B. It then reports
whether queries keep succeeding (Cloudflare tolerated the move) or start timing
out (it broke the connection).

Use two addresses from the SAME interface so egress filtering does not confound
the result, e.g. a wifi temporary + stable v6:

    ./quic-rebind-test cfflip \
      -a 2a00:8c40:238:232:9747:f8a5:8318:e880 \
      -b 2a00:8c40:238:232:91f4:28b8:74b2:f6e5

Flags: -sni (default cloudflare-dns.com), -ip (default 2606:4700:4700::1111),
-flip (default 6s), -n (default 20), -idle (default 30s).

## Other modes

    ./quic-rebind-test demo
        Self-contained: quic-go echo server + relay rebinding its source port
        every 4s + client. Shows quic-go's own server migrating. No network.

    ./quic-rebind-test natfwd <listen> <upstreamHost:port> [-every D] [-sources A,B,...]
        NAT-rebinding relay to put in front of any server. Rebinds the upstream
        4-tuple every D: a new source port, or cycling -sources addresses.

    ./quic-rebind-test serve <listenAddr>
    ./quic-rebind-test ping  <targetAddr> [-source IP] [-n N]
