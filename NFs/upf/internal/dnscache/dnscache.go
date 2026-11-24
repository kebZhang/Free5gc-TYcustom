// Package dnscache is a process-wide hostname -> IP cache for the UPF. [DNSCACHE 1124]
//
// Same code as Free5gc-TYcustom/pfcp/dnscache (the SMF's copy; go-upf does not use that
// pfcp module) except: the logger, and lookupFn resolves IPv4 only ("ip4"), as the old
// net.ResolveIPAddr("ip4", s.nodeID) in internal/pfcp/session.go did once per PFCP
// Session Establishment, on the single PFCP main goroutine (1123 run: 2.4-3.8 ms of the
// 3.3-4.9 ms Establishment service time). RefreshNow has no caller in the UPF.
//
// See SMFUPF_cacheDNS_PLAN1124.md at the Free5gc-TYcustom root.
package dnscache

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/free5gc/go-upf/internal/logger"
)

// Enabled=false: Lookup resolves on every call, nothing is cached, Start/RefreshNow are
// no-ops -- the behaviour before [DNSCACHE 1124].
const Enabled = true

// RefreshInterval is how often every cached host is re-resolved.
const RefreshInterval = time.Minute

// MinTriggerGap: a RefreshNow closer than this to the last accepted one is dropped.
const MinTriggerGap = 5 * time.Second

var (
	mu        sync.RWMutex
	cache     = map[string]net.IP{}
	gen       = map[string]uint64{} // per-host write counter; never deleted, only incremented
	startOnce sync.Once

	triggering  atomic.Bool  // a triggered refresh is running
	base        = time.Now() // monotonic reference, so a wall-clock step cannot block triggers
	lastTrigger atomic.Int64 // time.Since(base) of the last accepted trigger in ns; 0 = none yet

	// lookupFn is a package variable so tests can stub DNS. LookupIP goes through the
	// same singleflight as LookupIPAddr; "ip4" queries A records only, like the old
	// net.ResolveIPAddr("ip4", ...).
	lookupFn = func(ctx context.Context, host string) ([]net.IPAddr, error) {
		ips, err := net.DefaultResolver.LookupIP(ctx, "ip4", host)
		if err != nil {
			return nil, err
		}
		addrs := make([]net.IPAddr, 0, len(ips))
		for _, ip := range ips {
			addrs = append(addrs, net.IPAddr{IP: ip})
		}
		return addrs, nil
	}
)

// pick matches net.ResolveIPAddr("ip", host): the first IPv4 address, else the first one.
func pick(addrs []net.IPAddr) net.IP {
	for _, a := range addrs {
		if v4 := a.IP.To4(); v4 != nil {
			return v4
		}
	}
	if len(addrs) > 0 {
		return addrs[0].IP
	}
	return nil
}

func resolve(host string) (net.IP, error) {
	addrs, err := lookupFn(context.Background(), host)
	if err != nil {
		return nil, err
	}
	if ip := pick(addrs); ip != nil {
		return ip, nil
	}
	return nil, &net.DNSError{Err: "no address", Name: host, IsNotFound: true}
}

// Lookup returns the IP for host: an IP literal as-is, a cached host from the table,
// otherwise the result of one synchronous resolve, which is inserted only on success.
// The returned slice is the cached one; callers must not modify it.
func Lookup(host string) (net.IP, error) {
	if host == "" {
		return nil, &net.DNSError{Err: "empty host", Name: host, IsNotFound: true}
	}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return v4, nil // 4-byte, like net.ResolveIPAddr returns for an IPv4 literal
		}
		return ip, nil
	}
	if !Enabled {
		return resolve(host)
	}

	mu.RLock()
	ip, ok := cache[host]
	mu.RUnlock()
	if ok {
		return ip, nil
	}

	ip, err := resolve(host)
	if err != nil {
		return nil, err
	}
	mu.Lock()
	cache[host] = ip
	gen[host]++
	mu.Unlock()
	logger.PfcpLog.Infof("[DNSCACHE] miss: %s -> %s", host, ip)
	return ip, nil
}

// Prewarm resolves each host once so the first sessions do not miss. Empty strings and
// IP literals are skipped by Lookup itself; failures only warn.
func Prewarm(hosts ...string) {
	for _, h := range hosts {
		if h == "" {
			continue
		}
		if _, err := Lookup(h); err != nil {
			logger.PfcpLog.Warnf("[DNSCACHE] prewarm %s failed: %v", h, err)
		}
	}
}

// Start launches the RefreshInterval refresher once.
func Start() {
	if !Enabled {
		return
	}
	startOnce.Do(func() {
		go func() {
			t := time.NewTicker(RefreshInterval)
			defer t.Stop()
			for range t.C {
				refresh()
			}
		}()
	})
}

// RefreshNow runs one full refresh in the background, like Aether's RefreshDnsHostIpCache
// on a PFCP send failure (pfcp/message/send.go:464), but asynchronous and coalesced: at most
// one triggered refresh runs at a time, and a trigger within MinTriggerGap of the last
// accepted one is dropped, so a dead UPF failing hundreds of in-flight requests costs one
// refresh, not hundreds. Running at the same time as the periodic refresh is safe: the
// generation check in refresh handles it.
func RefreshNow() {
	if !Enabled {
		return
	}
	now := int64(time.Since(base))
	last := lastTrigger.Load()
	if last != 0 && now-last < int64(MinTriggerGap) {
		return
	}
	if !lastTrigger.CompareAndSwap(last, now) {
		return
	}
	if !triggering.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer triggering.Store(false)
		refresh()
	}()
}

func refresh() {
	// Snapshot each host together with its generation, then resolve without the lock
	// held, so a slow resolver never stalls the readers on the session path.
	mu.RLock()
	observed := make(map[string]uint64, len(cache))
	for h := range cache {
		observed[h] = gen[h]
	}
	mu.RUnlock()

	for h, g := range observed {
		ip, err := resolve(h)
		mu.Lock()
		// Apply only if nobody wrote this entry since the snapshot (a miss insert, the
		// other refresher). Comparing generations rather than values also stops a failed
		// lookup here from deleting an entry a concurrent refresh just installed.
		if gen[h] == g {
			if err != nil {
				delete(cache, h)
				logger.PfcpLog.Warnf("[DNSCACHE] refresh %s failed, entry deleted: %v", h, err)
			} else if !cache[h].Equal(ip) {
				logger.PfcpLog.Infof("[DNSCACHE] refresh %s: %s -> %s", h, cache[h], ip)
				cache[h] = ip
			}
			gen[h]++
		}
		mu.Unlock()
	}
}
