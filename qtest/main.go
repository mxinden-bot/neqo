// quic-rebind-test: probe how a QUIC/HTTP3 server reacts when the client's
// UDP source changes underneath a live connection: a source-port change (NAT
// rebinding) or a source-ADDRESS change (an IPv6 privacy-address rotation, or
// a wifi<->wired flip). Built to answer: does Cloudflare keep the connection
// alive, or break it?
//
// The client half of a QUIC connection cannot swap its socket mid-connection
// in quic-go, so the source change is done by a tiny in-process UDP relay that
// sits between the client and the real server and rebinds ITS upstream socket.
// From the server's point of view that is exactly a client whose 4-tuple moved.
//
// Modes:
//
//	cfflip   -a <ipv6A> -b <ipv6B> [-sni H] [-ip IP] [-flip D] [-n N]
//	    THE Cloudflare test. Opens one HTTP/3 (DoH) connection to Cloudflare,
//	    issues a DNS query every second, and partway through flips the upstream
//	    source address from A to B. Reports whether queries keep succeeding
//	    (Cloudflare migrated / tolerated it) or start timing out (it broke the
//	    connection). Use two addresses from the SAME interface (e.g. your two
//	    wifi v6 addresses) to isolate the server's behavior from egress
//	    filtering.
//
//	demo     Self-contained: quic-go echo server + relay that rebinds its
//	         source port every 4s + client. Proves quic-go's own server
//	         migrates. No network needed.
//
//	serve <listenAddr> | ping <targetAddr> [-source IP] [-n N] | natfwd <listen> <upstream> [-every D] [-sources A,B]
//	    Lower-level building blocks.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

const alpn = "quic-rebind-test"

func ts() string { return time.Now().Format("15:04:05.000") }

// Relay forwards <listen> <-> <upstream> and can rebind its upstream socket to
// a new source address/port on demand, simulating a moved client 4-tuple.
type Relay struct {
	client *net.UDPConn
	up     *net.UDPAddr
	cur    atomic.Pointer[net.UDPConn]
	cli    atomic.Pointer[net.UDPAddr]
}

func NewRelay(listen, upstream string, initial net.IP) (*Relay, error) {
	up, err := net.ResolveUDPAddr("udp", upstream)
	if err != nil {
		return nil, fmt.Errorf("upstream %s: %w", upstream, err)
	}
	la, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", listen, err)
	}
	c, err := net.ListenUDP("udp", la)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", listen, err)
	}
	r := &Relay{client: c, up: up}
	if err := r.Flip(initial); err != nil {
		return nil, err
	}
	go func() {
		b := make([]byte, 2048)
		for {
			n, ca, err := c.ReadFromUDP(b)
			if err != nil {
				return
			}
			r.cli.Store(ca)
			if u := r.cur.Load(); u != nil {
				u.Write(b[:n])
			}
		}
	}()
	return r, nil
}

func (r *Relay) ListenAddr() string { return r.client.LocalAddr().String() }

// Flip rebinds the upstream socket to a new source (nil = ephemeral port on the
// default source address).
func (r *Relay) Flip(src net.IP) error {
	var la *net.UDPAddr
	if src != nil {
		la = &net.UDPAddr{IP: src}
	}
	nu, err := net.DialUDP("udp", la, r.up)
	if err != nil {
		return fmt.Errorf("dial upstream (src=%v): %w", src, err)
	}
	old := r.cur.Swap(nu)
	go func() {
		b := make([]byte, 2048)
		for {
			n, err := nu.Read(b)
			if err != nil {
				return
			}
			if ca := r.cli.Load(); ca != nil {
				r.client.WriteToUDP(b[:n], ca)
			}
		}
	}()
	if old != nil {
		go func() { time.Sleep(150 * time.Millisecond); old.Close() }()
	}
	return nil
}

func mustIP(s string) net.IP {
	ip := net.ParseIP(s)
	if ip == nil {
		fmt.Printf("bad IP %q\n", s)
		os.Exit(2)
	}
	return ip
}

func cfflip() {
	fs := flag.NewFlagSet("cfflip", flag.ExitOnError)
	sni := fs.String("sni", "cloudflare-dns.com", "TLS server name / Host")
	ip := fs.String("ip", "2606:4700:4700::1111", "server IP to dial (Cloudflare DoH anycast v6)")
	a := fs.String("a", "", "source IPv6 address A (required)")
	b := fs.String("b", "", "source IPv6 address B to flip to (required)")
	flipAt := fs.Duration("flip", 6*time.Second, "flip A->B after this long")
	n := fs.Int("n", 20, "number of DoH queries (1/sec)")
	idle := fs.Duration("idle", 30*time.Second, "QUIC max idle timeout")
	fs.Parse(os.Args[2:])
	if *a == "" || *b == "" {
		fmt.Println("cfflip: -a and -b (two source IPv6 addresses) are required")
		fmt.Println("tip: use two addresses from the SAME interface, e.g. your wifi temporary + stable v6")
		os.Exit(2)
	}
	srcA, srcB := mustIP(*a), mustIP(*b)

	relay, err := NewRelay("[::1]:0", net.JoinHostPort(*ip, "443"), srcA)
	if err != nil {
		fmt.Printf("relay: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[cfflip] relay %s -> [%s]:443, starting source A=%s\n", relay.ListenAddr(), *ip, *a)
	fmt.Printf("[cfflip] target https://%s/dns-query, flip A->B at t+%s, B=%s\n", *sni, *flipAt, *b)

	tr := &http3.Transport{
		TLSClientConfig: &tls.Config{ServerName: *sni, NextProtos: []string{"h3"}},
		QUICConfig:      &quic.Config{MaxIdleTimeout: *idle, KeepAlivePeriod: 2 * time.Second},
		Dial: func(ctx context.Context, _ string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
			ra, _ := net.ResolveUDPAddr("udp", relay.ListenAddr())
			pc, err := net.ListenUDP("udp", nil)
			if err != nil {
				return nil, err
			}
			return quic.Dial(ctx, pc, ra, tlsCfg, cfg)
		},
	}
	defer tr.Close()
	client := &http.Client{Transport: tr}

	var flipped atomic.Bool
	go func() {
		time.Sleep(*flipAt)
		if err := relay.Flip(srcB); err != nil {
			fmt.Printf("[cfflip] FLIP FAILED: %v\n", err)
			return
		}
		flipped.Store(true)
		fmt.Printf("[cfflip] >>> FLIPPED upstream source A -> B (%s) at t=%s\n", *b, ts())
	}()

	url := fmt.Sprintf("https://%s/dns-query?name=example.com&type=A", *sni)
	okBefore, okAfter, failAfter := 0, 0, 0
	for i := 1; i <= *n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		req.Header.Set("accept", "application/dns-json")
		start := time.Now()
		resp, err := client.Do(req)
		phase := "pre-flip "
		if flipped.Load() {
			phase = "POST-flip"
		}
		if err != nil {
			fmt.Printf("[cfflip] q%-2d %s FAIL after %4dms: %v\n", i, phase, time.Since(start).Milliseconds(), err)
			if flipped.Load() {
				failAfter++
			}
		} else {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			fmt.Printf("[cfflip] q%-2d %s ok   %4dms status=%d\n", i, phase, time.Since(start).Milliseconds(), resp.StatusCode)
			if flipped.Load() {
				okAfter++
			} else {
				okBefore++
			}
		}
		cancel()
		time.Sleep(time.Second)
	}
	fmt.Printf("\n[cfflip] RESULT: pre-flip ok=%d | post-flip ok=%d fail=%d\n", okBefore, okAfter, failAfter)
	switch {
	case okAfter > 0 && failAfter == 0:
		fmt.Println("[cfflip] => Cloudflare TOLERATED the source change (connection survived).")
	case okAfter == 0 && failAfter > 0:
		fmt.Println("[cfflip] => Cloudflare BROKE the connection after the source flip.")
	default:
		fmt.Println("[cfflip] => mixed/inconclusive (some recovered). Check egress filtering on B.")
	}
}

// ---- building blocks ----

func serverTLS() *tls.Config {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		DNSNames:     []string{"localhost"},
	}
	der, _ := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		NextProtos:   []string{alpn},
	}
}

func readFull(s *quic.Stream, b []byte) error {
	for got := 0; got < len(b); {
		n, err := s.Read(b[got:])
		got += n
		if err != nil {
			return err
		}
	}
	return nil
}

func runServer(listen string) *quic.Listener {
	ln, err := quic.ListenAddr(listen, serverTLS(), &quic.Config{MaxIdleTimeout: 30 * time.Second})
	if err != nil {
		fmt.Printf("[server] listen %s: %v\n", listen, err)
		os.Exit(1)
	}
	go func() {
		for {
			conn, err := ln.Accept(context.Background())
			if err != nil {
				return
			}
			fmt.Printf("[server] accepted from %s (t=%s)\n", conn.RemoteAddr(), ts())
			go func(conn *quic.Conn) {
				last := conn.RemoteAddr().String()
				for conn.Context().Err() == nil {
					time.Sleep(150 * time.Millisecond)
					if cur := conn.RemoteAddr().String(); cur != last {
						fmt.Printf("[server] >>> peer MIGRATED %s -> %s (t=%s)\n", last, cur, ts())
						last = cur
					}
				}
			}(conn)
			go func(conn *quic.Conn) {
				str, err := conn.AcceptStream(context.Background())
				if err != nil {
					return
				}
				b := make([]byte, 8)
				for {
					if err := readFull(str, b); err != nil {
						return
					}
					str.Write(b)
				}
			}(conn)
		}
	}()
	return ln
}

func runClient(target, source string, n int, idle time.Duration) {
	var la *net.UDPAddr
	if source != "" {
		la = &net.UDPAddr{IP: mustIP(source)}
	}
	pc, err := net.ListenUDP("udp", la)
	if err != nil {
		fmt.Printf("[client] bind: %v\n", err)
		os.Exit(1)
	}
	tgt, _ := net.ResolveUDPAddr("udp", target)
	conn, err := quic.Dial(context.Background(), pc, tgt,
		&tls.Config{InsecureSkipVerify: true, NextProtos: []string{alpn}},
		&quic.Config{MaxIdleTimeout: idle, KeepAlivePeriod: 2 * time.Second})
	if err != nil {
		fmt.Printf("[client] dial: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[client] connected %s -> %s\n", conn.LocalAddr(), conn.RemoteAddr())
	str, _ := conn.OpenStreamSync(context.Background())
	var lastOK atomic.Uint64
	go func() {
		b := make([]byte, 8)
		for {
			if err := readFull(str, b); err != nil {
				return
			}
			lastOK.Store(binary.BigEndian.Uint64(b))
		}
	}()
	b := make([]byte, 8)
	for i := uint64(1); i <= uint64(n); i++ {
		binary.BigEndian.PutUint64(b, i)
		str.Write(b)
		time.Sleep(time.Second)
		fmt.Printf("[client] sent %d, last echo %d\n", i, lastOK.Load())
	}
}

func natfwd() {
	fs := flag.NewFlagSet("natfwd", flag.ExitOnError)
	every := fs.Duration("every", 4*time.Second, "rebind interval (0=never)")
	sources := fs.String("sources", "", "comma-separated local source IPs to cycle (default: new port)")
	fs.Parse(os.Args[4:])
	var srcs []net.IP
	if *sources != "" {
		for _, s := range strings.Split(*sources, ",") {
			srcs = append(srcs, mustIP(strings.TrimSpace(s)))
		}
	}
	var first net.IP
	if len(srcs) > 0 {
		first = srcs[0]
	}
	r, err := NewRelay(os.Args[2], os.Args[3], first)
	if err != nil {
		fmt.Printf("relay: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[natfwd] %s -> %s (t=%s)\n", r.ListenAddr(), os.Args[3], ts())
	if *every > 0 {
		for i := 1; ; i++ {
			time.Sleep(*every)
			var src net.IP
			if len(srcs) > 0 {
				src = srcs[i%len(srcs)]
			}
			if err := r.Flip(src); err != nil {
				fmt.Printf("[natfwd] flip: %v\n", err)
			} else {
				fmt.Printf("[natfwd] >>> REBIND upstream (src=%v) at t=%s\n", src, ts())
			}
		}
	}
	select {}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: quic-rebind-test {cfflip|demo|serve|ping|natfwd} ...")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "cfflip":
		cfflip()
	case "demo":
		ln := runServer("127.0.0.1:0")
		r, err := NewRelay("127.0.0.1:0", ln.Addr().String(), nil)
		if err != nil {
			fmt.Printf("relay: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("[demo] server %s, relay %s (rebinds source port every 4s)\n", ln.Addr(), r.ListenAddr())
		go func() {
			for {
				time.Sleep(4 * time.Second)
				r.Flip(nil)
				fmt.Printf("[demo] >>> REBIND upstream source port at t=%s\n", ts())
			}
		}()
		runClient(r.ListenAddr(), "", 24, 30*time.Second)
	case "serve":
		runServer(os.Args[2])
		select {}
	case "ping":
		fs := flag.NewFlagSet("ping", flag.ExitOnError)
		source := fs.String("source", "", "local source IP")
		n := fs.Int("n", 30, "pings")
		idle := fs.Duration("idle", 30*time.Second, "idle timeout")
		fs.Parse(os.Args[3:])
		runClient(fs.Arg(0), *source, *n, *idle)
	case "natfwd":
		natfwd()
	default:
		fmt.Printf("unknown mode %q\n", os.Args[1])
		os.Exit(2)
	}
}
