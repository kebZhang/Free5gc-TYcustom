package dnscache

// [DNSCACHE 1124] unit tests with a stubbed resolver (no real DNS).

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type stub struct {
	calls atomic.Int64
	ip    atomic.Value // string; "" = fail
}

func (s *stub) fn(_ context.Context, _ string) ([]net.IPAddr, error) {
	s.calls.Add(1)
	v, _ := s.ip.Load().(string)
	if v == "" {
		return nil, errors.New("stub: lookup failed")
	}
	return []net.IPAddr{{IP: net.ParseIP(v)}}, nil
}

func setup(t *testing.T, ip string) *stub {
	t.Helper()
	s := &stub{}
	s.ip.Store(ip)
	mu.Lock()
	cache = map[string]net.IP{}
	gen = map[string]uint64{}
	mu.Unlock()
	lastTrigger.Store(0)
	old := lookupFn
	lookupFn = s.fn
	t.Cleanup(func() { lookupFn = old })
	return s
}

func cached(h string) (net.IP, bool) {
	mu.RLock()
	defer mu.RUnlock()
	ip, ok := cache[h]
	return ip, ok
}

func TestLiteralBypassesCache(t *testing.T) {
	s := setup(t, "10.0.0.1")
	ip, err := Lookup("192.168.1.2")
	if err != nil || !ip.Equal(net.ParseIP("192.168.1.2")) || s.calls.Load() != 0 {
		t.Fatalf("got %v %v, calls %d", ip, err, s.calls.Load())
	}
	if _, ok := cached("192.168.1.2"); ok {
		t.Fatal("literal must not be cached")
	}
}

func TestMissThenHit(t *testing.T) {
	s := setup(t, "10.0.0.1")
	for i := 0; i < 3; i++ {
		ip, err := Lookup("upf")
		if err != nil || !ip.Equal(net.ParseIP("10.0.0.1")) {
			t.Fatalf("lookup %d: %v %v", i, ip, err)
		}
	}
	if n := s.calls.Load(); n != 1 {
		t.Fatalf("resolver called %d times, want 1", n)
	}
	if ip, _ := Lookup("upf"); len(ip) != net.IPv4len {
		t.Fatalf("want 4-byte IPv4, got %d bytes", len(ip))
	}
}

func TestFailureNotCached(t *testing.T) {
	s := setup(t, "")
	if _, err := Lookup("upf"); err == nil {
		t.Fatal("want error")
	}
	if _, ok := cached("upf"); ok {
		t.Fatal("failure must not be cached")
	}
	_, _ = Lookup("upf")
	if n := s.calls.Load(); n != 2 {
		t.Fatalf("resolver called %d times, want 2 (no negative caching)", n)
	}
}

func TestRefreshDeletesOnFailure(t *testing.T) {
	s := setup(t, "10.0.0.1")
	_, _ = Lookup("upf")
	s.ip.Store("")
	refresh()
	if _, ok := cached("upf"); ok {
		t.Fatal("entry should be deleted after a failed refresh")
	}
}

func TestRefreshUpdatesChangedIP(t *testing.T) {
	s := setup(t, "10.0.0.1")
	_, _ = Lookup("upf")
	s.ip.Store("10.0.0.2")
	refresh()
	if ip, ok := cached("upf"); !ok || !ip.Equal(net.ParseIP("10.0.0.2")) {
		t.Fatalf("want 10.0.0.2, got %v %v", ip, ok)
	}
}

func TestRefreshDiscardsResultWhenEntryWrittenMeanwhile(t *testing.T) {
	setup(t, "10.0.0.1")
	_, _ = Lookup("upf")
	// While refresh resolves (lock not held), another writer installs 10.0.0.9; the
	// refresh's own (failed) result must then be discarded, not delete the new entry.
	lookupFn = func(context.Context, string) ([]net.IPAddr, error) {
		mu.Lock()
		cache["upf"] = net.ParseIP("10.0.0.9").To4()
		gen["upf"]++
		mu.Unlock()
		return nil, errors.New("stub: lookup failed")
	}
	refresh()
	if ip, ok := cached("upf"); !ok || !ip.Equal(net.ParseIP("10.0.0.9")) {
		t.Fatalf("concurrent write lost: %v %v", ip, ok)
	}
}

func TestRefreshNowCoalesces(t *testing.T) {
	s := setup(t, "10.0.0.1")
	_, _ = Lookup("upf") // 1 call
	for i := 0; i < 20; i++ {
		RefreshNow()
	}
	deadline := time.Now().Add(2 * time.Second)
	for triggering.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if triggering.Load() {
		t.Fatal("triggered refresh did not finish")
	}
	if n := s.calls.Load(); n != 2 {
		t.Fatalf("resolver called %d times, want 2 (1 miss + 1 coalesced refresh)", n)
	}
}
