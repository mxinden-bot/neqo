// tiny local HTTP/3 server for validating cfmigrate's socket-swap end to end.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

func main() {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		DNSNames:     []string{"localhost"},
	}
	der, _ := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}

	mux := http.NewServeMux()
	mux.HandleFunc("/dns-query", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprintf(w, `{"Status":0}`)
	})
	s := &http3.Server{
		Addr:            "127.0.0.1:443",
		TLSConfig:       &tls.Config{Certificates: []tls.Certificate{cert}},
		Handler:         mux,
		QUICConfig:      &quic.Config{MaxIdleTimeout: 30 * time.Second},
		EnableDatagrams: false,
	}
	fmt.Println("http3 test server on 127.0.0.1:443")
	panic(s.ListenAndServe())
}
