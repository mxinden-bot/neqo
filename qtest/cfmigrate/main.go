// cfmigrate: a real quic-go HTTP/3 client (no relay) that opens one DoH
// connection to Cloudflare, sends from IPv6 address A, then rebinds its own
// UDP socket to IPv6 address B mid-connection, and reports whether the
// connection survives (Cloudflare followed the move) or dies (it did not).
//
// quic-go ties a connection to one net.PacketConn, so to change the client's
// source address without a relay we hand quic-go a swappable PacketConn whose
// underlying UDP socket we replace on the fly. On the flip we CLOSE the old
// socket, so if Cloudflare keeps replying to A those replies are lost and the
// connection stalls: that is the honest test of whether it migrates to B.
//
// Usage:
//
//	cfmigrate -a <ipv6A> -b <ipv6B> [-sni cloudflare-dns.com] [-ip 2606:4700:4700::1111] [-flip 6s] [-n 20]
//
// Use two addresses on the SAME interface (e.g. a wifi temporary + stable v6)
// so source-address egress filtering does not confound the result.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// swapConn is a net.PacketConn whose underlying UDP socket can be replaced at
// runtime. Reads from the current socket are funnelled through a channel so
// quic-go's read loop is decoupled from socket swaps; writes go out the
// currently-active socket (hence the current source address).
type swapConn struct {
	rx     chan rxPacket
	closed chan struct{}

	mu       sync.Mutex
	active   *net.UDPConn
	deadline time.Time
}

type rxPacket struct {
	data []byte
	addr net.Addr
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func newSwapConn(src net.IP) (*swapConn, error) {
	c := &swapConn{rx: make(chan rxPacket, 256), closed: make(chan struct{})}
	if err := c.swap(src); err != nil {
		return nil, err
	}
	return c, nil
}

// swap binds a new UDP socket on src, makes it active, and closes the old one.
func (c *swapConn) swap(src net.IP) error {
	u, err := net.ListenUDP("udp", &net.UDPAddr{IP: src})
	if err != nil {
		return fmt.Errorf("bind %v: %w", src, err)
	}
	c.mu.Lock()
	old := c.active
	c.active = u
	c.mu.Unlock()
	go c.reader(u)
	if old != nil {
		old.Close() // its reader goroutine exits; late replies to the old source are dropped
	}
	return nil
}

func (c *swapConn) reader(u *net.UDPConn) {
	buf := make([]byte, 2048)
	for {
		n, addr, err := u.ReadFrom(buf)
		if err != nil {
			return // socket closed on swap or shutdown
		}
		d := make([]byte, n)
		copy(d, buf[:n])
		select {
		case c.rx <- rxPacket{d, addr}:
		case <-c.closed:
			return
		}
	}
}

func (c *swapConn) ReadFrom(p []byte) (int, net.Addr, error) {
	c.mu.Lock()
	dl := c.deadline
	c.mu.Unlock()
	var timer <-chan time.Time
	if !dl.IsZero() {
		d := time.Until(dl)
		if d <= 0 {
			return 0, nil, timeoutError{}
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timer = t.C
	}
	select {
	case pkt := <-c.rx:
		return copy(p, pkt.data), pkt.addr, nil
	case <-timer:
		return 0, nil, timeoutError{}
	case <-c.closed:
		return 0, nil, net.ErrClosed
	}
}

func (c *swapConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	c.mu.Lock()
	u := c.active
	c.mu.Unlock()
	return u.WriteTo(p, addr)
}

func (c *swapConn) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	c.mu.Lock()
	u := c.active
	c.mu.Unlock()
	if u != nil {
		return u.Close()
	}
	return nil
}

func (c *swapConn) LocalAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active.LocalAddr()
}

func (c *swapConn) SetDeadline(t time.Time) error      { return c.SetReadDeadline(t) }
func (c *swapConn) SetWriteDeadline(t time.Time) error { return nil }
func (c *swapConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	return nil
}

func ts() string { return time.Now().Format("15:04:05.000") }

func main() {
	sni := flag.String("sni", "cloudflare-dns.com", "TLS server name / Host")
	ip := flag.String("ip", "2606:4700:4700::1111", "Cloudflare DoH server IP to dial")
	a := flag.String("a", "", "source IPv6 address A (required)")
	b := flag.String("b", "", "source IPv6 address B to move to (required)")
	flipAt := flag.Duration("flip", 6*time.Second, "move A->B after this long")
	n := flag.Int("n", 20, "number of DoH queries (1/sec)")
	idle := flag.Duration("idle", 30*time.Second, "QUIC max idle timeout")
	insecure := flag.Bool("insecure", false, "skip TLS verification (local testing only)")
	flag.Parse()
	if *a == "" || *b == "" {
		fmt.Println("cfmigrate: -a and -b (two source IPv6 addresses) are required")
		os.Exit(2)
	}
	srcA, srcB := net.ParseIP(*a), net.ParseIP(*b)
	if srcA == nil || srcB == nil {
		fmt.Println("cfmigrate: -a/-b must be valid IPs")
		os.Exit(2)
	}

	sc, err := newSwapConn(srcA)
	if err != nil {
		fmt.Printf("bind A: %v\n", err)
		os.Exit(1)
	}
	defer sc.Close()
	server := &net.UDPAddr{IP: net.ParseIP(*ip), Port: 443}
	fmt.Printf("[cfmigrate] source A=%s -> [%s]:443, will move to B=%s at t+%s\n", *a, *ip, *b, *flipAt)

	tr := &http3.Transport{
		TLSClientConfig: &tls.Config{ServerName: *sni, NextProtos: []string{"h3"}, InsecureSkipVerify: *insecure},
		QUICConfig:      &quic.Config{MaxIdleTimeout: *idle, KeepAlivePeriod: 2 * time.Second},
		Dial: func(ctx context.Context, _ string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
			return quic.Dial(ctx, sc, server, tlsCfg, cfg)
		},
	}
	defer tr.Close()
	client := &http.Client{Transport: tr}

	var moved atomic.Bool
	go func() {
		time.Sleep(*flipAt)
		if err := sc.swap(srcB); err != nil {
			fmt.Printf("[cfmigrate] move to B FAILED: %v\n", err)
			return
		}
		moved.Store(true)
		fmt.Printf("[cfmigrate] >>> now sending from B=%s (old socket closed) at t=%s\n", *b, ts())
	}()

	url := fmt.Sprintf("https://%s/dns-query?name=example.com&type=A", *sni)
	var okBefore, okAfter, failAfter int
	for i := 1; i <= *n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		req.Header.Set("accept", "application/dns-json")
		start := time.Now()
		resp, err := client.Do(req)
		phase := "A"
		if moved.Load() {
			phase = "B"
		}
		if err != nil {
			fmt.Printf("[cfmigrate] q%-2d src=%s FAIL %4dms: %v\n", i, phase, time.Since(start).Milliseconds(), errShort(err))
			if moved.Load() {
				failAfter++
			}
		} else {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			fmt.Printf("[cfmigrate] q%-2d src=%s ok   %4dms status=%d local=%s\n", i, phase, time.Since(start).Milliseconds(), resp.StatusCode, sc.LocalAddr())
			if moved.Load() {
				okAfter++
			} else {
				okBefore++
			}
		}
		cancel()
		time.Sleep(time.Second)
	}
	fmt.Printf("\n[cfmigrate] RESULT: from-A ok=%d | from-B ok=%d fail=%d\n", okBefore, okAfter, failAfter)
	switch {
	case okAfter > 0 && failAfter == 0:
		fmt.Printf("[cfmigrate] => %s FOLLOWED the source-address change (connection survived).\n", *sni)
	case okAfter == 0 && failAfter > 0:
		fmt.Printf("[cfmigrate] => %s did NOT follow it (connection broke after moving to B).\n", *sni)
	default:
		fmt.Println("[cfmigrate] => mixed: some recovered. Suspect egress filtering on B, not the server.")
	}
}

func errShort(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.New("timeout (no response)")
	}
	return err
}
