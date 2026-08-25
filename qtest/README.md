# quic-rebind-test / cfmigrate

Throwaway probes (parked here for easy transfer, unrelated to neqo itself) for
how a QUIC/HTTP3 server reacts when the client's UDP source changes under a live
connection.

## cfmigrate: real client, no relay (send from IPv6 A, then IPv6 B)

`cfmigrate/` is a real quic-go HTTP/3 client with no relay. It opens one DoH
connection to Cloudflare, sends from source address A, then rebinds its own UDP
socket to source address B mid-connection (closing the A socket), and reports
whether the connection survives. quic-go ties a connection to one PacketConn, so
the swap is done by handing quic-go a PacketConn whose underlying UDP socket we
replace on the fly. Closing the old socket is deliberate: if the server keeps
replying to A those replies are lost, so this is an honest test of whether it
migrates to B.

    cd cfmigrate && go build -o cfmigrate .        # or use the checked-in binary
    ./cfmigrate -a <ipv6A> -b <ipv6B>

Example, two addresses on the SAME interface (avoids egress-filtering confounds):

    ./cfmigrate \
      -a 2a00:8c40:238:232:9747:f8a5:8318:e880 \
      -b 2a00:8c40:238:232:91f4:28b8:74b2:f6e5

Flags: -sni (cloudflare-dns.com), -ip (2606:4700:4700::1111), -flip (6s),
-n (20), -idle (30s), -insecure (local testing).

Validated over IPv4 loopback against `testserver/` (a quic-go HTTP/3 server,
which does migrate): the client moves its source port mid-connection and every
request after the move still succeeds.

## quic-rebind-test: relay-based variants (port rebind, cycle source addrs)

`quic-rebind-test` (prebuilt binary in this dir) drives the change with an
in-process UDP relay instead, which lets you rebind the source PORT in front of
any server, or cycle a list of source addresses, and put it in front of curl /
Firefox:

    ./quic-rebind-test demo                                   # local, shows quic-go migrating
    ./quic-rebind-test natfwd <listen> <upstream> [-every D] [-sources A,B]
    ./quic-rebind-test cfflip -a <ipv6A> -b <ipv6B>           # relay version of the cfmigrate test
