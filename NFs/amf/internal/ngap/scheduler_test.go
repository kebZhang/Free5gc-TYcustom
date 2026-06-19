package ngap

import (
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockAddr is a trivial net.Addr so worker logging (which reads RemoteAddr)
// does not nil-panic.
type mockAddr struct{ id string }

func (a *mockAddr) Network() string { return "sctp" }
func (a *mockAddr) String() string  { return a.id }

// mockConn represents one SCTP association. Distinct pointers model distinct
// gNB connections, which is exactly the routing key the scheduler uses.
type mockConn struct {
	net.Conn
	addr mockAddr
}

func newMockConn(id string) *mockConn { return &mockConn{addr: mockAddr{id: id}} }

func (m *mockConn) Close() error                       { return nil }
func (m *mockConn) Read(b []byte) (n int, err error)   { return 0, nil }
func (m *mockConn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (m *mockConn) RemoteAddr() net.Addr               { return &m.addr }
func (m *mockConn) LocalAddr() net.Addr                { return &m.addr }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestWorkerForConn_Stickiness(t *testing.T) {
	// The same SCTP association must always map to the same worker.
	numWorkers := 8
	scheduler := NewUEScheduler(numWorkers, 100, func(conn net.Conn, msg []byte) {})
	defer scheduler.Shutdown()

	conns := []*mockConn{
		newMockConn("gnb-1"), newMockConn("gnb-2"), newMockConn("gnb-3"),
	}

	for _, c := range conns {
		first := scheduler.workerForConn(c)
		for i := 0; i < 100; i++ {
			assert.Equal(t, first, scheduler.workerForConn(c),
				"connection %s should always pin to the same worker", c.addr.id)
		}
	}
}

func TestWorkerForConn_RoundRobinDistribution(t *testing.T) {
	// New associations are assigned round-robin, so all workers are used evenly
	// even when many gNBs connect.
	numWorkers := 8
	scheduler := NewUEScheduler(numWorkers, 100, func(conn net.Conn, msg []byte) {})
	defer scheduler.Shutdown()

	numConns := 8000
	distribution := make(map[int]int)
	for i := 0; i < numConns; i++ {
		c := newMockConn(string(rune('a'+i%26)) + string(rune(i)))
		distribution[scheduler.workerForConn(c)]++
	}

	assert.Equal(t, numWorkers, len(distribution), "all workers should receive associations")

	expectedPerWorker := numConns / numWorkers
	for workerID, count := range distribution {
		t.Logf("Worker %d received %d associations (expected %d)", workerID, count, expectedPerWorker)
		// Round-robin gives an exactly even split.
		assert.Equal(t, expectedPerWorker, count, "Worker %d distribution off", workerID)
	}
}

func TestWorkerForConn_Range(t *testing.T) {
	numWorkers := 8
	scheduler := NewUEScheduler(numWorkers, 100, func(conn net.Conn, msg []byte) {})
	defer scheduler.Shutdown()

	for i := 0; i < 1000; i++ {
		idx := scheduler.workerForConn(newMockConn(string(rune(i))))
		assert.GreaterOrEqual(t, idx, 0)
		assert.Less(t, idx, numWorkers)
	}
}

func TestReleaseConn_RemovesMapping(t *testing.T) {
	// After a connection closes, ReleaseConn must drop the mapping so the map
	// does not leak across many gNB churn cycles.
	scheduler := NewUEScheduler(4, 100, func(conn net.Conn, msg []byte) {})
	defer scheduler.Shutdown()

	c := newMockConn("gnb-x")
	scheduler.workerForConn(c)

	scheduler.mu.Lock()
	_, present := scheduler.connToWorker[c]
	scheduler.mu.Unlock()
	require.True(t, present, "mapping should exist after first dispatch")

	scheduler.ReleaseConn(c)

	scheduler.mu.Lock()
	_, present = scheduler.connToWorker[c]
	size := len(scheduler.connToWorker)
	scheduler.mu.Unlock()
	assert.False(t, present, "mapping should be removed after ReleaseConn")
	assert.Equal(t, 0, size, "no associations should remain")
}

func TestScheduler_PerAssociationOrdering(t *testing.T) {
	// All messages on one association are processed in arrival order.
	numWorkers := 4
	numMessages := 200

	var processedOrder []int
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(numMessages)

	handler := func(conn net.Conn, msg []byte) {
		seqNum := int(msg[0])<<8 | int(msg[1])
		mu.Lock()
		processedOrder = append(processedOrder, seqNum)
		mu.Unlock()
		wg.Done()
	}

	scheduler := NewUEScheduler(numWorkers, 1000, handler)
	defer scheduler.Shutdown()
	scheduler.NotifyActiveConns(2) // latch dGNB (per-association) mode

	conn := newMockConn("gnb-ordered")
	for i := 0; i < numMessages; i++ {
		scheduler.DispatchTask(Task{
			Conn:    conn,
			Message: []byte{byte(i >> 8), byte(i)},
		})
	}

	wg.Wait()

	require.Equal(t, numMessages, len(processedOrder))
	for i := 0; i < numMessages; i++ {
		assert.Equal(t, i, processedOrder[i], "message %d should be processed in order", i)
	}
}

func TestScheduler_MultipleAssociationsConcurrent(t *testing.T) {
	// Many gNB connections process concurrently, each preserving its own order.
	numWorkers := 8
	numConns := 20
	messagesPerConn := 50

	processedByConn := make(map[string][]int)
	var mu sync.Mutex

	var processingWg sync.WaitGroup
	processingWg.Add(numConns * messagesPerConn)

	handler := func(conn net.Conn, msg []byte) {
		connID := conn.RemoteAddr().String()
		seqNum := int(msg[0])
		mu.Lock()
		processedByConn[connID] = append(processedByConn[connID], seqNum)
		mu.Unlock()
		processingWg.Done()
	}

	scheduler := NewUEScheduler(numWorkers, 1000, handler)
	defer scheduler.Shutdown()
	scheduler.NotifyActiveConns(numConns) // latch dGNB (per-association) mode

	var wg sync.WaitGroup
	wg.Add(numConns)
	for c := 0; c < numConns; c++ {
		go func(connIdx int) {
			defer wg.Done()
			conn := newMockConn("gnb-" + string(rune('A'+connIdx)))
			for m := 0; m < messagesPerConn; m++ {
				scheduler.DispatchTask(Task{Conn: conn, Message: []byte{byte(m)}})
			}
		}(c)
	}
	wg.Wait()
	processingWg.Wait()

	for c := 0; c < numConns; c++ {
		connID := "gnb-" + string(rune('A'+c))
		messages := processedByConn[connID]
		require.Equal(t, messagesPerConn, len(messages), "conn %s missing messages", connID)
		for i := 0; i < messagesPerConn; i++ {
			assert.Equal(t, i, messages[i], "conn %s message %d out of order", connID, i)
		}
	}
}

func TestScheduler_GracefulShutdown(t *testing.T) {
	numWorkers := 4
	numTasks := 50

	var processedCount int32
	var processingWg sync.WaitGroup
	processingWg.Add(numTasks)

	handler := func(conn net.Conn, msg []byte) {
		atomic.AddInt32(&processedCount, 1)
		time.Sleep(10 * time.Millisecond)
		processingWg.Done()
	}

	scheduler := NewUEScheduler(numWorkers, 100, handler)

	conn := newMockConn("gnb-shutdown")
	for i := 0; i < numTasks; i++ {
		scheduler.DispatchTask(Task{Conn: conn, Message: []byte{0x00}})
	}

	scheduler.Shutdown()
	processingWg.Wait()

	processed := atomic.LoadInt32(&processedCount)
	t.Logf("Processed %d tasks before shutdown", processed)
	assert.Equal(t, int32(numTasks), processed,
		"All queued tasks should be processed during graceful shutdown")
}

func TestScheduler_WorkerCount(t *testing.T) {
	testCases := []struct {
		name          string
		numWorkers    int
		expectedCount int
	}{
		{"Single worker", 1, 1},
		{"Four workers", 4, 4},
		{"Eight workers", 8, 8},
		{"Auto-detect (0)", 0, runtime.NumCPU() * 3}, // 0 => runtime.NumCPU() * 3
		{"Auto-detect (negative)", -1, runtime.NumCPU() * 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheduler := NewUEScheduler(tc.numWorkers, 100,
				func(conn net.Conn, msg []byte) {})
			defer scheduler.Shutdown()

			actualCount := len(scheduler.workers)
			assert.Equal(t, tc.expectedCount, actualCount,
				"Worker count should match expected")
		})
	}
}

func TestScheduler_UsedWorkerCount(t *testing.T) {
	// In dGNB mode each new association is pinned round-robin to a worker, so
	// dispatching on N distinct connections (N <= pool size) must exercise
	// exactly N distinct workers, counted once each.
	numWorkers := 4
	var wg sync.WaitGroup

	// Side effect of markWorkerUsed writes worker_num.txt; keep it in a temp dir.
	t.Chdir(t.TempDir())

	handler := func(conn net.Conn, msg []byte) { wg.Done() }
	scheduler := NewUEScheduler(numWorkers, 100, handler)
	defer scheduler.Shutdown()
	scheduler.NotifyActiveConns(2) // latch per-association routing

	assert.Equal(t, 0, scheduler.UsedWorkerCount(),
		"no workers used before any dispatch")

	conns := []net.Conn{
		newMockConn("gnb-1"), newMockConn("gnb-2"),
		newMockConn("gnb-3"), newMockConn("gnb-4"),
	}
	// Dispatch twice per connection: the second hit on the same worker must not
	// be double-counted.
	wg.Add(len(conns) * 2)
	for round := 0; round < 2; round++ {
		for _, c := range conns {
			scheduler.DispatchTask(Task{Conn: c, Message: []byte{0x00}})
		}
	}
	wg.Wait()

	assert.Equal(t, numWorkers, scheduler.UsedWorkerCount(),
		"each distinct association should map to a distinct worker, counted once")
}

func TestResolveWorkerPoolSize(t *testing.T) {
	testCases := []struct {
		name       string
		configured int
		expected   int
	}{
		{"Explicit positive", 8, 8},
		{"Explicit one", 1, 1},
		{"Auto-detect (0)", 0, runtime.NumCPU() * 3},
		{"Auto-detect (negative)", -1, runtime.NumCPU() * 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, ResolveWorkerPoolSize(tc.configured),
				"Resolved worker count should match expected")
		})
	}
}

func TestScheduler_DGNBLatch_Sticky(t *testing.T) {
	// dGNB mode is off by default, turns on at >= 2 active associations, and
	// never falls back even when the count drops to 1 or 0.
	scheduler := NewUEScheduler(4, 100, func(conn net.Conn, msg []byte) {})
	defer scheduler.Shutdown()

	assert.False(t, scheduler.IsDGNBMode(), "should start in non-dGNB mode")

	scheduler.NotifyActiveConns(1)
	assert.False(t, scheduler.IsDGNBMode(), "1 association must not latch dGNB")

	scheduler.NotifyActiveConns(2)
	assert.True(t, scheduler.IsDGNBMode(), ">= 2 associations must latch dGNB")

	// Tail of the run: associations close, count drops -- must stay latched.
	scheduler.NotifyActiveConns(1)
	scheduler.NotifyActiveConns(0)
	assert.True(t, scheduler.IsDGNBMode(), "dGNB latch must be sticky (no fallback)")
}

func TestScheduler_NonDGNB_HashByUE(t *testing.T) {
	// In the default (non-dGNB) mode, routing must match the original behaviour:
	// hash by UE ID, so the same UE is sticky to one worker while different UEs
	// of the same single connection can spread across workers.
	numWorkers := 8
	scheduler := NewUEScheduler(numWorkers, 100, func(conn net.Conn, msg []byte) {})
	defer scheduler.Shutdown()

	require.False(t, scheduler.IsDGNBMode(), "must be non-dGNB by default")

	// Same UE -> same worker, regardless of which connection carries it.
	for _, ueID := range []uint64{1, 7, 42, 1000, 65535} {
		want := scheduler.hashUEID(ueID)
		for i := 0; i < 50; i++ {
			assert.Equal(t, want, scheduler.hashUEID(ueID),
				"UE %d must always hash to the same worker", ueID)
		}
	}

	// Different UEs spread across more than one worker (intra-connection
	// parallelism that the dGNB mode deliberately gives up).
	used := make(map[int]bool)
	for ueID := uint64(0); ueID < 100; ueID++ {
		used[scheduler.hashUEID(ueID)] = true
	}
	assert.Greater(t, len(used), 1, "different UEs should use multiple workers in non-dGNB mode")
}

func TestScheduler_NonDGNB_PerUEOrdering(t *testing.T) {
	// Non-dGNB mode: messages of one UE are processed in order even when many
	// UEs share a single SCTP connection.
	numWorkers := 8
	numUEs := 16
	msgsPerUE := 40

	processedByUE := make(map[uint64][]int)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(numUEs * msgsPerUE)

	handler := func(conn net.Conn, msg []byte) {
		ueID := uint64(msg[0])
		seq := int(msg[1])
		mu.Lock()
		processedByUE[ueID] = append(processedByUE[ueID], seq)
		mu.Unlock()
		wg.Done()
	}

	scheduler := NewUEScheduler(numWorkers, 1000, handler)
	defer scheduler.Shutdown()
	require.False(t, scheduler.IsDGNBMode(), "must stay non-dGNB (single connection)")

	conn := newMockConn("single-gnb") // one connection carrying many UEs
	var feed sync.WaitGroup
	feed.Add(numUEs)
	for u := 0; u < numUEs; u++ {
		go func(ueID uint64) {
			defer feed.Done()
			for m := 0; m < msgsPerUE; m++ {
				scheduler.DispatchTask(Task{UEID: ueID, Conn: conn, Message: []byte{byte(ueID), byte(m)}})
			}
		}(uint64(u))
	}
	feed.Wait()
	wg.Wait()

	for u := uint64(0); u < uint64(numUEs); u++ {
		msgs := processedByUE[u]
		require.Equal(t, msgsPerUE, len(msgs), "UE %d missing messages", u)
		for i := 0; i < msgsPerUE; i++ {
			assert.Equal(t, i, msgs[i], "UE %d message %d out of order", u, i)
		}
	}
}
