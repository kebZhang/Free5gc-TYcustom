package ngap

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/free5gc/amf/internal/logger"
	"github.com/free5gc/amf/internal/recvtime"
)

// workerNumFilePath is where the peak (maximum) number of distinct NGAP workers
// exercised during a run is written. Pinned to an absolute path so it lands in
// the AMF pod's /tmp regardless of the process working directory.
const workerNumFilePath = "/tmp/worker_num.txt"

// Task represents a work item to be processed by a worker.
// It carries the SCTP connection, the raw NGAP message, and the UE identifier
// extracted from it. Routing uses one of two strategies depending on the
// detected scenario (see UEScheduler):
//   - non-dGNB (single SCTP association): hash by UEID, so different UEs of the
//     same gNB run in parallel on different workers (the original behaviour).
//   - dGNB (>= 2 associations): pin by SCTP association, so each gNB's messages
//     stay ordered on a single worker and the RAN-UE-NGAP-ID (which is not
//     unique across gNBs) is not used for routing.
type Task struct {
	UEID     uint64    // AMF-UE-NGAP-ID or RAN-UE-NGAP-ID (used only in non-dGNB mode)
	Conn     net.Conn  // The SCTP association this message arrived on
	Message  []byte    // The raw NGAP message bytes
	RecvTime time.Time // Time the message was returned by SCTPRead; used only for AMF_log
}

// Worker represents a goroutine that processes tasks from its dedicated queue.
type Worker struct {
	ID       int
	taskChan chan Task
	stopChan chan struct{} // Signal channel for shutdown
	stopOnce sync.Once     // Ensures stopChan is closed only once
	handler  func(conn net.Conn, msg []byte)
	wg       *sync.WaitGroup
}

// NewWorker creates and starts a new worker goroutine.
func NewWorker(id int, bufferSize int, handler func(conn net.Conn, msg []byte), wg *sync.WaitGroup) *Worker {
	w := &Worker{
		ID:       id,
		taskChan: make(chan Task, bufferSize),
		stopChan: make(chan struct{}),
		handler:  handler,
		wg:       wg,
	}
	wg.Add(1)
	go w.run()
	return w
}

// run is the main event loop for the worker.
func (w *Worker) run() {
	defer func() {
		if p := recover(); p != nil {
			logger.NgapLog.Errorf("Worker %d panic: %v", w.ID, p)
		}
		w.wg.Done()
	}()
	logger.NgapLog.Infof("Worker %d started", w.ID)

	for {
		select {
		case task := <-w.taskChan:
			logger.NgapLog.Debugf("Worker %d processing message from %v (per-association ordering)",
				w.ID, task.Conn.RemoteAddr())
			recvtime.Set(task.RecvTime)
			w.handler(task.Conn, task.Message)
			recvtime.Clear()

		case <-w.stopChan:
			logger.NgapLog.Infof("Worker %d: shutdown signal received, draining queue...", w.ID)
			w.drainAndExit()
			return
		}
	}
}

// drainAndExit consumes remaining tasks in the buffer without blocking.
func (w *Worker) drainAndExit() {
	for {
		select {
		case task := <-w.taskChan:
			logger.NgapLog.Debugf("Worker %d processing residual message from %v", w.ID, task.Conn.RemoteAddr())
			recvtime.Set(task.RecvTime)
			w.handler(task.Conn, task.Message)
			recvtime.Clear()
		default:
			// Channel is empty, exit safely
			logger.NgapLog.Infof("Worker %d: queue drained, stopped.", w.ID)
			return
		}
	}
}

// Submit submits a task to this worker's queue.
// Returns true if the task was successfully queued, false if the worker is stopped.
func (w *Worker) Submit(task Task) bool {
	select {
	case w.taskChan <- task:
		// Successfully queued (blocks here if buffer is full, providing backpressure)
		return true
	case <-w.stopChan:
		// Worker stopped (either before submission or while waiting). Unblock and return false.
		logger.NgapLog.Warnf("Worker %d stopped, rejecting message from %v", w.ID, task.Conn.RemoteAddr())
		return false
	}
}

// Stop signals the worker to shut down.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopChan)
	})
}

// UEScheduler distributes NGAP tasks to workers using one of two strategies,
// selected automatically by the number of active SCTP associations.
//
// Non-dGNB (the default, single association): tasks are hashed by UEID, so
// different UEs of the same gNB are spread across workers and processed in
// parallel. This is the original AMF behaviour and is preserved unchanged.
//
// dGNB (>= 2 associations observed): each SCTP association is pinned to a single
// worker the first time a message is seen on it, so all NGAP messages of one
// gNB are processed in arrival order by that worker, while new connections are
// assigned round-robin to spread load. Routing then does not depend on the
// RAN-UE-NGAP-ID, which is not unique across gNBs.
//
// The dGNB decision is sticky: once >= 2 active associations are observed the
// scheduler latches into dGNB mode for the rest of the process lifetime and
// never falls back, so that the tail of a dGNB run (where associations close
// one by one and the active count drops back towards 1) cannot flip routing
// mid-association and reorder messages.
type UEScheduler struct {
	workers    []*Worker
	numWorkers int
	wg         sync.WaitGroup

	// dGNBLatched is 0 until >= 2 active SCTP associations are observed, then 1
	// permanently. Read/written with atomics so the dispatch hot path is lock-free.
	dGNBLatched int32

	mu           sync.Mutex       // Guards connToWorker and nextWorker
	connToWorker map[net.Conn]int // SCTP association -> assigned worker index (dGNB mode)
	nextWorker   int              // Round-robin cursor for new associations (dGNB mode)

	// usedFlags[i] is 0 until worker i processes its first task, then 1. Used to
	// count, without double-counting, how many distinct workers have actually
	// been exercised during the run (e.g. while PacketRusher registers UEs).
	usedFlags []int32
	usedCount int32 // Number of distinct workers used so far (atomic).
}

// maxWorkerPoolSize is the hard upper bound on the NGAP worker pool size. Both a
// configured value and the auto-detected default are clamped to this, so the pool
// can never grow without bound (which would blow up memory: each worker owns a
// channel of ngapTaskBufferSize Task slots). 10000 is far above any realistic UE
// fan-out and exists purely as a safety ceiling.
const maxWorkerPoolSize = 10000

// ResolveWorkerPoolSize returns the effective NGAP worker count for a configured
// value. A configured value > 0 is used as-is; otherwise it falls back to
// runtime.NumCPU() * 60. The result is clamped to maxWorkerPoolSize. This is the
// single source of truth for the default rule, so callers (e.g. init.go) can log
// the real worker count before the scheduler is built.
func ResolveWorkerPoolSize(configured int) int {
	size := configured
	if size <= 0 {
		size = runtime.NumCPU() * 60
	}
	if size > maxWorkerPoolSize {
		size = maxWorkerPoolSize
	}
	return size
}

// NewUEScheduler creates a new scheduler with the specified number of workers.
func NewUEScheduler(numWorkers int, taskBufferSize int, handler func(conn net.Conn, msg []byte)) *UEScheduler {
	numWorkers = ResolveWorkerPoolSize(numWorkers)

	logger.NgapLog.Infof("Initializing NGAP Scheduler with %d workers "+
		"(hash-by-UE by default; auto-switches to per-association on >=2 SCTP associations)", numWorkers)

	scheduler := &UEScheduler{
		workers:      make([]*Worker, numWorkers),
		numWorkers:   numWorkers,
		connToWorker: make(map[net.Conn]int),
		usedFlags:    make([]int32, numWorkers),
	}

	for i := 0; i < numWorkers; i++ {
		scheduler.workers[i] = NewWorker(i, taskBufferSize, handler, &scheduler.wg)
	}

	return scheduler
}

// NotifyActiveConns reports the current number of active SCTP associations to
// the scheduler. Once it observes >= 2, it latches permanently into dGNB mode.
// Called from the NGAP read path before each dispatch; cheap and lock-free in
// the common (already-latched) case.
func (s *UEScheduler) NotifyActiveConns(active int) {
	if active >= 2 && atomic.LoadInt32(&s.dGNBLatched) == 0 {
		if atomic.CompareAndSwapInt32(&s.dGNBLatched, 0, 1) {
			logger.NgapLog.Infof("Detected %d active SCTP associations: latching into dGNB mode "+
				"(per-association routing)", active)
		}
	}
}

// IsDGNBMode reports whether the scheduler has latched into dGNB mode.
func (s *UEScheduler) IsDGNBMode() bool {
	return atomic.LoadInt32(&s.dGNBLatched) == 1
}

// hashUEID maps a UE ID to a worker index. Used in non-dGNB mode so that all
// messages of the same UE go to the same worker (per-UE ordering) while
// different UEs spread across workers.
func (s *UEScheduler) hashUEID(ueID uint64) int {
	return int(ueID % uint64(s.numWorkers))
}

// workerForConn returns the worker index assigned to the given SCTP association,
// assigning one round-robin on first sight. The assignment is sticky for the
// lifetime of the connection so that one gNB's messages stay ordered.
func (s *UEScheduler) workerForConn(conn net.Conn) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if idx, ok := s.connToWorker[conn]; ok {
		return idx
	}

	idx := s.nextWorker
	s.nextWorker = (s.nextWorker + 1) % s.numWorkers
	s.connToWorker[conn] = idx

	logger.NgapLog.Infof("Assigned SCTP association %v to Worker %d (%d active associations)",
		conn.RemoteAddr(), idx, len(s.connToWorker))
	return idx
}

// DispatchTask routes a task to a worker. In dGNB mode it pins by SCTP
// association; otherwise it hashes by UEID (original behaviour).
func (s *UEScheduler) DispatchTask(task Task) bool {
	var workerIndex int
	if s.IsDGNBMode() {
		workerIndex = s.workerForConn(task.Conn)
		logger.NgapLog.Debugf("Dispatching message from %v to Worker %d (per-association routing)",
			task.Conn.RemoteAddr(), workerIndex)
	} else {
		workerIndex = s.hashUEID(task.UEID)
		logger.NgapLog.Debugf("Dispatching UE ID %d to Worker %d (hash-based routing)",
			task.UEID, workerIndex)
	}

	s.markWorkerUsed(workerIndex)

	worker := s.workers[workerIndex]
	return worker.Submit(task)
}

// markWorkerUsed records that worker idx has handled at least one task. The
// first time a given worker is used, the distinct-used count is incremented and
// the new total is written to worker_num.txt. This makes the file reflect the
// peak number of workers actually exercised during a run (e.g. how many of the
// pool PacketRusher's registrations end up spreading across). Subsequent hits on
// an already-counted worker are a single atomic load and do no I/O.
func (s *UEScheduler) markWorkerUsed(idx int) {
	if atomic.CompareAndSwapInt32(&s.usedFlags[idx], 0, 1) {
		used := atomic.AddInt32(&s.usedCount, 1)
		logger.NgapLog.Infof("NGAP worker %d used for the first time; %d/%d workers now in use",
			idx, used, s.numWorkers)
		s.writeUsedWorkerCount(int(used))
	}
}

// UsedWorkerCount returns how many distinct workers have processed at least one
// task so far.
func (s *UEScheduler) UsedWorkerCount() int {
	return int(atomic.LoadInt32(&s.usedCount))
}

// writeUsedWorkerCount overwrites workerNumFilePath with the current
// distinct-used worker count. The count is monotonically increasing, so the file
// always reflects the peak (i.e. the maximum) number of workers exercised so far.
// Called only when the count changes (at most numWorkers times per run), so it
// does not add per-message I/O on the dispatch hot path.
func (s *UEScheduler) writeUsedWorkerCount(used int) {
	content := fmt.Sprintf("used_workers=%d\npool_size=%d\n", used, s.numWorkers)
	if err := os.WriteFile(workerNumFilePath, []byte(content), 0o644); err != nil {
		logger.NgapLog.Warnf("Failed to write %s: %v", workerNumFilePath, err)
	}
}

// ReleaseConn removes the worker mapping for a closed SCTP association.
// It must be called when a gNB connection is torn down so the map does not grow
// without bound. Pending tasks already queued for the worker are unaffected.
func (s *UEScheduler) ReleaseConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if idx, ok := s.connToWorker[conn]; ok {
		delete(s.connToWorker, conn)
		logger.NgapLog.Infof("Released SCTP association %v from Worker %d (%d active associations)",
			conn.RemoteAddr(), idx, len(s.connToWorker))
	}
}

// Shutdown gracefully shuts down all workers.
func (s *UEScheduler) Shutdown() {
	logger.NgapLog.Info("Shutting down NGAP Scheduler and all workers...")

	for i, worker := range s.workers {
		logger.NgapLog.Infof("Closing task channel for Worker %d", i)
		worker.Stop()
	}

	s.wg.Wait()
	logger.NgapLog.Info("All workers shut down successfully")
}

// Global scheduler instance
var (
	globalScheduler     *UEScheduler
	globalSchedulerOnce sync.Once
	schedulerMutex      sync.RWMutex
)

// InitScheduler initializes the global NGAP scheduler.
// Should be called once during AMF startup.
func InitScheduler(numWorkers int, taskBufferSize int, handler func(conn net.Conn, msg []byte)) {
	globalSchedulerOnce.Do(func() {
		// Apply sensible defaults if invalid values provided
		numWorkers = ResolveWorkerPoolSize(numWorkers)
		if taskBufferSize <= 0 {
			taskBufferSize = 4096 // Default buffer size
		}

		schedulerMutex.Lock()
		defer schedulerMutex.Unlock()

		globalScheduler = NewUEScheduler(numWorkers, taskBufferSize, handler)
		logger.NgapLog.Infof("Global NGAP Scheduler initialized with %d workers, buffer size %d",
			numWorkers, taskBufferSize)
	})
}

// GetScheduler returns the global scheduler instance.
func GetScheduler() (*UEScheduler, error) {
	schedulerMutex.RLock()
	defer schedulerMutex.RUnlock()

	if globalScheduler == nil {
		return nil, fmt.Errorf("scheduler not initialized")
	}
	return globalScheduler, nil
}

// ShutdownScheduler gracefully shuts down the global scheduler.
func ShutdownScheduler() {
	schedulerMutex.Lock()
	defer schedulerMutex.Unlock()

	if globalScheduler != nil {
		globalScheduler.Shutdown()
	}
}
