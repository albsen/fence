package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// skipIfCannotBind is defined in upstream_test.go (same package).

// spawnProxy starts an HTTP proxy on 127.0.0.1 with the given route decision
// and optional total-request timeout (0 = use the default). It returns the
// proxy's listen address and registers cleanup.
func spawnProxy(t *testing.T, route RouteFunc, upstream *url.URL, requestTimeout time.Duration) string {
	t.Helper()
	skipIfCannotBind(t)

	p := NewHTTPProxy(route, upstream, false, false)
	if requestTimeout != 0 {
		p.SetRequestTimeout(requestTimeout)
	}
	port, err := p.Start()
	if err != nil {
		t.Fatalf("proxy start: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// mustURL parses a URL for test use, failing the test on error.
func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatalf("parse url %q: %v", s, err)
	}
	return u
}

// proxyClient builds an http.Client that routes through the given proxy
// address, as a sandboxed client would.
func proxyClient(t *testing.T, proxyAddr string) *http.Client {
	t.Helper()
	return &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(mustURL(t, "http://"+proxyAddr))}}
}

// --- SSE origin helpers ---------------------------------------------------

const (
	// sseCount is the number of SSE events the slow origin emits.
	sseCount = 320
	// sseInterval is the gap between events. sseCount*interval ≈ 32s, which is
	// comfortably beyond the legacy 30s client timeout this test guards.
	sseInterval = 100 * time.Millisecond
)

var sseEventRE = regexp.MustCompile(`data: event-(\d+)`)

// sseHandler streams sseCount Server-Sent-Events, one every sseInterval, then
// closes the connection.
func sseHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fl, _ := w.(http.Flusher)
	for i := 1; i <= sseCount; i++ {
		fmt.Fprintf(w, "data: event-%d\n\n", i)
		if fl != nil {
			fl.Flush()
		}
		time.Sleep(sseInterval)
	}
}

// countSSEEvents counts "data: event-N" lines in a captured SSE body.
func countSSEEvents(data string) int {
	return len(sseEventRE.FindAllString(data, -1))
}

// longSSEEnabled gates the >30s streaming tests, which opt in via
// FENCE_TEST_SSE_LONG=1 so the default `go test ./...` stays fast.
func longSSEEnabled() bool {
	return os.Getenv("FENCE_TEST_SSE_LONG") == "1"
}

// drainTo collects the full response body on a goroutine and sends it on ch.
func drainTo(body io.Reader, ch chan<- string) {
	go func() {
		data, _ := io.ReadAll(body)
		ch <- string(data)
	}()
}

// --- Tests ----------------------------------------------------------------

// TestStreamingSSE_Direct verifies that a long-running SSE response streamed
// through the direct connection path is not cut off by any total-request
// timeout: the client receives every event and the request runs for the full
// duration of the stream.
func TestStreamingSSE_Direct(t *testing.T) {
	if !longSSEEnabled() {
		t.Skip("set FENCE_TEST_SSE_LONG=1 to run the >30s streaming tests")
	}
	skipIfCannotBind(t)

	originLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("origin listen: %v", err)
	}
	origin := &http.Server{Handler: http.HandlerFunc(sseHandler), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = origin.Serve(originLn) }()
	defer func() { _ = origin.Close() }() //nolint:errcheck
	originPort := originLn.Addr().(*net.TCPAddr).Port

	// The proxy routes the origin to the direct path and is given a 5s
	// non-streaming request timeout. The SSE response must be exempt from it.
	route := func(host string, port int) RouteDecision { return RouteDecisionDirect }
	proxyAddr := spawnProxy(t, route, nil, 5*time.Second)
	pc := proxyClient(t, proxyAddr)

	req, _ := http.NewRequest(http.MethodGet,
		fmt.Sprintf("http://127.0.0.1:%d/v1/stream", originPort), nil)
	req.Header.Set("Accept", "text/event-stream")

	start := time.Now()
	resp, err := pc.Do(req)
	if err != nil {
		t.Fatalf("streaming request via proxy: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}

	body := make(chan string, 1)
	drainTo(resp.Body, body)
	data := <-body
	elapsed := time.Since(start)

	if got := countSSEEvents(data); got != sseCount {
		t.Errorf("received %d events, want %d (stream was truncated)", got, sseCount)
	}
	want := sseCount * sseInterval
	if elapsed < want-500*time.Millisecond {
		t.Errorf("stream ended after %v, want ~%v (client cut off the stream)", elapsed, want)
	}
	if elapsed > want+15*time.Second {
		t.Errorf("stream took %v, far longer than %v", elapsed, want)
	}
}

// TestStreamingSSE_Upstream verifies the same property when the request is
// forwarded to an upstream proxy (grey-zone path).
func TestStreamingSSE_Upstream(t *testing.T) {
	if !longSSEEnabled() {
		t.Skip("set FENCE_TEST_SSE_LONG=1 to run the >30s streaming tests")
	}
	skipIfCannotBind(t)

	originLn, _ := net.Listen("tcp", "127.0.0.1:0")
	origin := &http.Server{Handler: http.HandlerFunc(sseHandler), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = origin.Serve(originLn) }()
	defer func() { _ = origin.Close() }() //nolint:errcheck
	originPort := originLn.Addr().(*net.TCPAddr).Port

	// A minimal upstream HTTP proxy that forwards the request to the origin and
	// streams the response back, flushing as data arrives.
	upstreamLn, _ := net.Listen("tcp", "127.0.0.1:0")
	upstream := &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			target := "http://127.0.0.1:" + strconv.Itoa(originPort) + r.URL.Path
			oreq, _ := http.NewRequest(r.Method, target, r.Body)
			oreq.Header.Set("Accept", "text/event-stream")
			oresp, err := http.DefaultClient.Do(oreq)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer func() { _ = oresp.Body.Close() }()
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(oresp.StatusCode)
			fl, _ := w.(http.Flusher)
			buf := make([]byte, 4096)
			for {
				n, rerr := oresp.Body.Read(buf)
				if n > 0 {
					_, _ = w.Write(buf[:n])
					if fl != nil {
						fl.Flush()
					}
				}
				if rerr != nil {
					return
				}
			}
		}),
	}
	go func() { _ = upstream.Serve(upstreamLn) }()
	defer func() { _ = upstream.Close() }() //nolint:errcheck

	route := func(host string, port int) RouteDecision { return RouteDecisionUpstream }
	upstreamURL := mustURL(t, "http://"+upstreamLn.Addr().String())
	proxyAddr := spawnProxy(t, route, upstreamURL, 5*time.Second)
	pc := proxyClient(t, proxyAddr)

	req, _ := http.NewRequest(http.MethodGet,
		fmt.Sprintf("http://127.0.0.1:%d/v1/stream", originPort), nil)
	req.Header.Set("Accept", "text/event-stream")

	start := time.Now()
	resp, err := pc.Do(req)
	if err != nil {
		t.Fatalf("streaming request via upstream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body := make(chan string, 1)
	drainTo(resp.Body, body)
	data := <-body
	elapsed := time.Since(start)

	if got := countSSEEvents(data); got != sseCount {
		t.Errorf("received %d events, want %d (stream was truncated)", got, sseCount)
	}
	if want := sseCount * sseInterval; elapsed < want-500*time.Millisecond {
		t.Errorf("stream ended after %v, want ~%v", elapsed, want)
	}
}

// TestStreamingSSE_Cancelled verifies that cancelling the caller's request
// mid-stream tears the proxy's copy down promptly rather than running to the
// end of the stream.
func TestStreamingSSE_Cancelled(t *testing.T) {
	skipIfCannotBind(t)

	const burst = 20
	originLn, _ := net.Listen("tcp", "127.0.0.1:0")
	origin := &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			fl, _ := w.(http.Flusher)
			for i := 1; i <= burst; i++ {
				fmt.Fprintf(w, "data: burst-%d\n\n", i)
				if fl != nil {
					fl.Flush()
				}
			}
			// Keep the connection open; the proxy is mid-copy when we cancel.
			<-r.Context().Done()
		}),
	}
	go func() { _ = origin.Serve(originLn) }()
	defer func() { _ = origin.Close() }() //nolint:errcheck
	originPort := originLn.Addr().(*net.TCPAddr).Port

	route := func(host string, port int) RouteDecision { return RouteDecisionDirect }
	proxyAddr := spawnProxy(t, route, nil, 0)
	pc := proxyClient(t, proxyAddr)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequest(http.MethodGet,
		fmt.Sprintf("http://127.0.0.1:%d/v1/stream", originPort), nil)
	req.Header.Set("Accept", "text/event-stream")
	req = req.WithContext(ctx)

	done := make(chan struct{}, 1)
	go func() {
		defer func() { done <- struct{}{} }()
		resp, err := pc.Do(req)
		if err != nil {
			return
		}
		defer func() { _ = resp.Body.Close() }()
		buf := make([]byte, 64<<10)
		for {
			if _, rerr := resp.Body.Read(buf); rerr != nil {
				return
			}
		}
	}()

	// Let the burst flow, then cancel while the proxy is still copying.
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("proxy did not tear down the streaming copy after cancellation")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("cancellation took %v, want prompt teardown", d)
	}
}

// TestNonStreaming_ReturnsFullBody verifies an ordinary (non-streaming) HTTP
// request is proxied normally and its full body arrives.
func TestNonStreaming_ReturnsFullBody(t *testing.T) {
	skipIfCannotBind(t)

	originLn, _ := net.Listen("tcp", "127.0.0.1:0")
	origin := &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("hello plain http"))
		}),
	}
	go func() { _ = origin.Serve(originLn) }()
	defer func() { _ = origin.Close() }() //nolint:errcheck
	originPort := originLn.Addr().(*net.TCPAddr).Port

	route := func(host string, port int) RouteDecision { return RouteDecisionDirect }
	proxyAddr := spawnProxy(t, route, nil, 10*time.Second)
	pc := proxyClient(t, proxyAddr)

	resp, err := pc.Get(fmt.Sprintf("http://127.0.0.1:%d/plain", originPort))
	if err != nil {
		t.Fatalf("request via proxy: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(data) != "hello plain http" {
		t.Errorf("body = %q, want %q", data, "hello plain http")
	}
}

// TestNonStreaming_TimesOutWhenSlow verifies that a non-streaming response
// whose body exceeds the configured total-request timeout is cut off, while a
// fast one completes. This is the safeguard the 30s removal must preserve.
func TestNonStreaming_TimesOutWhenSlow(t *testing.T) {
	skipIfCannotBind(t)

	t.Run("slow body exceeds timeout", func(t *testing.T) {
		originLn, _ := net.Listen("tcp", "127.0.0.1:0")
		origin := &http.Server{
			ReadHeaderTimeout: 10 * time.Second,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Send headers, then trickle the body slowly so the total
				// timeout fires during the body read.
				w.WriteHeader(http.StatusOK)
				fl, _ := w.(http.Flusher)
				if fl != nil {
					fl.Flush()
				}
				for i := 0; i < 10; i++ {
					_, _ = w.Write([]byte("x"))
					if fl != nil {
						fl.Flush()
					}
					time.Sleep(150 * time.Millisecond)
				}
			}),
		}
		go func() { _ = origin.Serve(originLn) }()
		defer func() { _ = origin.Close() }() //nolint:errcheck
		originPort := originLn.Addr().(*net.TCPAddr).Port

		route := func(host string, port int) RouteDecision { return RouteDecisionDirect }
		proxyAddr := spawnProxy(t, route, nil, 400*time.Millisecond)
		pc := proxyClient(t, proxyAddr)

		start := time.Now()
		resp, err := pc.Get(fmt.Sprintf("http://127.0.0.1:%d/plain", originPort))
		elapsed := time.Since(start)
		if err == nil {
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode == http.StatusOK {
				data, _ := io.ReadAll(resp.Body)
				// The timeout should have cut the body short; the full body
				// would be 10 bytes.
				if len(data) >= 10 {
					t.Fatalf("full body (%d bytes) received despite 400ms total-request timeout", len(data))
				}
			}
		}
		// Expect the request to end well before the ~1.5s full body, bounded
		// by the 400ms timeout (plus connection overhead).
		if elapsed < 300*time.Millisecond || elapsed > 2*time.Second {
			t.Errorf("slow request ended in %v, want ~400ms timeout", elapsed)
		}
	})

	t.Run("fast body within timeout", func(t *testing.T) {
		originLn, _ := net.Listen("tcp", "127.0.0.1:0")
		origin := &http.Server{
			ReadHeaderTimeout: 10 * time.Second,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("quick"))
			}),
		}
		go func() { _ = origin.Serve(originLn) }()
		defer func() { _ = origin.Close() }() //nolint:errcheck
		originPort := originLn.Addr().(*net.TCPAddr).Port

		route := func(host string, port int) RouteDecision { return RouteDecisionDirect }
		proxyAddr := spawnProxy(t, route, nil, 5*time.Second)
		pc := proxyClient(t, proxyAddr)

		resp, err := pc.Get(fmt.Sprintf("http://127.0.0.1:%d/plain", originPort))
		if err != nil {
			t.Fatalf("fast request: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if data, _ := io.ReadAll(resp.Body); string(data) != "quick" {
			t.Errorf("body = %q, want %q", data, "quick")
		}
	})
}

// TestUnresponsiveUpstream_HeaderTimeout verifies that an upstream which
// accepts the connection but never sends response headers is bounded by the
// response-header timeout (returns 502) rather than hanging indefinitely.
func TestUnresponsiveUpstream_HeaderTimeout(t *testing.T) {
	skipIfCannotBind(t)

	// A listener that accepts connections and then stays silent.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	silentPort := ln.Addr().(*net.TCPAddr).Port
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			// Keep the connection open and never send a response line, so the
			// proxy's ResponseHeaderTimeout fires.
			time.Sleep(10 * time.Second)
			_ = c.Close()
		}
	}()

	orig := responseHeaderTimeout
	responseHeaderTimeout = 400 * time.Millisecond
	t.Cleanup(func() { responseHeaderTimeout = orig })

	route := func(host string, port int) RouteDecision { return RouteDecisionDirect }
	proxyAddr := spawnProxy(t, route, nil, 10*time.Second)
	pc := proxyClient(t, proxyAddr)

	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/x", silentPort), nil)
	start := time.Now()
	resp, err := pc.Do(req)
	elapsed := time.Since(start)
	if err == nil {
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", resp.StatusCode)
		}
	}
	// The header timeout (400ms) should dominate; allow generous slack.
	if elapsed < 300*time.Millisecond || elapsed > 3*time.Second {
		t.Errorf("unresponsive upstream handled in %v, want ~400ms header timeout", elapsed)
	}
}

// TestConnectFailure_Direct verifies a direct connection failure (port closed)
// surfaces as a 502 Bad Gateway (connection-establishment failure path).
func TestConnectFailure_Direct(t *testing.T) {
	skipIfCannotBind(t)

	// Find a closed port by binding and closing.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	route := func(host string, p int) RouteDecision { return RouteDecisionDirect }
	proxyAddr := spawnProxy(t, route, nil, 0)
	pc := proxyClient(t, proxyAddr)

	resp, err := pc.Get(fmt.Sprintf("http://127.0.0.1:%d/plain", port))
	if err != nil {
		return // client surfaced the failure; acceptable
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

// TestReadProxyResponseStatus_SSEStream sanity-checks that an SSE response is
// detected as streaming from the request headers (the code path the timeout
// exemption depends on), without needing a long-running origin.
func TestIsStreamingRequest(t *testing.T) {
	tests := []struct {
		name string
		acc  string
		want bool
	}{
		{"sse accept", "text/event-stream", true},
		{"sse accept with charset", "text/event-stream;charset=utf-8", true},
		{"sse accept in list", "application/json, text/event-stream", true},
		{"json accept", "application/json", false},
		{"no accept", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, "http://x/", nil)
			if tt.acc != "" {
				req.Header.Set("Accept", tt.acc)
			}
			if got := isStreamingRequest(req); got != tt.want {
				t.Errorf("isStreamingRequest(accept=%q) = %v, want %v", tt.acc, got, tt.want)
			}
		})
	}
}
