// Package accesslog provides low-overhead, asynchronous logging of every
// outgoing HTTP request/response (client/requester view) and every NF<->MongoDB
// interaction, for offline timestamp-based analysis.
//
// Design (high throughput, no torn/interleaved writes, order-insensitive):
//   - Hot path only marshals a small record and pushes it onto a buffered
//     channel; it never touches the file and never blocks on I/O.
//   - A single dedicated writer goroutine drains the channel and appends to the
//     file. Because there is exactly one writer, lines can never interleave.
//   - Output is JSON Lines (one JSON object per line) so records can be parsed
//     and sorted by timestamp afterwards regardless of write order.
//
// Files (override with env vars):
//   - HTTP_LOG_PATH (default /tmp/HTTP_log.txt)
//   - DB_LOG_PATH   (default /tmp/DB_log.txt)
//
// All timestamps are recorded from the NF's (requester's) point of view.
package accesslog

import (
	"bufio"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

// srcNF is the name of the NF this binary runs as (the requester for HTTP logs,
// and the "NF" side for DB logs). Set once at package init.
const srcNF = "AUSF"

// recKind distinguishes which file a record belongs to.
type recKind uint8

const (
	kindHTTP recKind = iota
	kindDB
)

// record is a single log entry queued for the writer goroutine. It already
// carries the fully-formatted JSON line to keep the writer goroutine trivial
// and to do the (cheap) formatting work off the single writer for parallelism.
type record struct {
	kind recKind
	line *[]byte // pooled; see linePool. Owned by the writer once enqueued.
}

// linePool recycles the buffers that carry formatted lines from the producing
// goroutine to the writer.
//
// With the initial capacities corrected, each line costs exactly one allocation,
// and at the rates this experiment runs at that is the largest single source of
// garbage in the process. Recycling removes it.
//
// Ownership is strictly linear: a builder takes a buffer, fills it, and hands it
// to enqueue; from that moment only the writer touches it, and the writer
// returns it once the bytes have been copied into the file's bufio. A record
// dropped because the queue was full simply never comes back, which is correct
// and cheaper than trying to reclaim it. Nothing is ever returned twice, so two
// goroutines can never end up writing into the same buffer.
var linePool sync.Pool

// getLine returns an empty buffer with room for at least capHint bytes.
func getLine(capHint int) *[]byte {
	if v := linePool.Get(); v != nil {
		bp := v.(*[]byte)
		if cap(*bp) >= capHint {
			*bp = (*bp)[:0]
			return bp
		}
		// Recycled buffer is too small for this kind of line. Size it correctly
		// now rather than letting append reallocate part-way through and copy
		// what has already been written; the pointer wrapper is still reused.
		*bp = make([]byte, 0, capHint)
		return bp
	}
	b := make([]byte, 0, capHint)
	return &b
}

func putLine(bp *[]byte) {
	*bp = (*bp)[:0]
	linePool.Put(bp)
}

const (
	// queueCapacity bounds memory use. With large-memory experiment hosts this
	// is sized to absorb sustained bursts before falling back to drop-on-full.
	queueCapacity = 1 << 21 // 2097152
	writerBufferSize = 1 << 20 // 1 MiB

	// httpLineCap is the initial capacity for a client HTTP line. Measured on
	// C6525100g_NF1HTTP_500ms_3logs_0827v1 (RQ2000/UE1000, nine-point build):
	// client lines were p50 533 B, p99 572 B, max 573 B. The M2 field added by
	// HTTP_10thlog_0903.md costs 58 B, and stream_id / latency_us can each still
	// gain a digit over that longest line, so the real ceiling is ~633 B. 704 is
	// the next Go size class up and leaves ~70 B of headroom.
	//
	// Do not inflate this "to be safe": a bigger capHint costs nothing per line
	// in steady state (getLine just reslices a pooled buffer) but linePool is
	// shared by every record kind and each queued record holds its own buffer, so
	// the burst-memory ceiling is queueCapacity * capHint.
	httpLineCap = 704

	envHTTPPath = "HTTP_LOG_PATH"
	envDBPath   = "DB_LOG_PATH"

	defaultHTTPPath = "/tmp/HTTP_log.txt"
	defaultDBPath   = "/tmp/DB_log.txt"
)

// wQueueCapacity bounds the W event queue. It is deliberately far smaller than
// queueCapacity: a Go channel whose element type contains pointers has its whole
// buffer scanned on every GC cycle, whether or not it holds anything, so an
// oversized queue is a permanent GC cost rather than free insurance. At most one
// W event exists per response, the collector only formats JSON, and a full queue
// costs a counted drop rather than any stall -- so a queue this size is ample.
const wQueueCapacity = 1 << 16 // 65536

var (
	queue    chan record
	dropped  atomic.Uint64      // count of records dropped because the queue was full
	initOne  sync.Once
	flushReq chan chan struct{} // request a synchronous flush from the writer

	// wQueue carries response-header-flushed events from the HTTP/2 socket
	// writer to wCollectorLoop, which turns them into log lines. The socket
	// writer must never format JSON or block, hence the hand-off.
	wQueue    chan http2.ResponseHeadersFlushedEvent
	wFlushReq chan chan struct{}
)

// Init starts the background writer. It is safe to call multiple times; only the
// first call has any effect. It is invoked automatically on first use, but NFs
// may call it explicitly at startup.
func Init() {
	initOne.Do(func() {
		queue = make(chan record, queueCapacity)
		flushReq = make(chan chan struct{})
		wQueue = make(chan http2.ResponseHeadersFlushedEvent, wQueueCapacity)
		wFlushReq = make(chan chan struct{})
		go writerLoop()
		go wCollectorLoop()
	})
}

// WEventSink returns the channel to hand to http2.Server.ResponseHeadersFlushed.
//
// It calls Init first on purpose. A nil channel here would be silently fatal:
// the non-blocking send inside the fork would always take its default branch and
// the run would produce no W events at all. Package initialisation already calls
// Init, but this makes the sink independent of import ordering.
func WEventSink() chan<- http2.ResponseHeadersFlushedEvent {
	Init()
	return wQueue
}

// WDropped reports how many W events were lost, either because this queue was
// full or because a connection's marker ring overflowed. The counter lives in
// the fork, next to where the drops happen.
func WDropped() uint64 { return http2.ResponseHeadersFlushedDrops() }

// WAccountingErrors reports failures of the fork's internal self-check on the W
// byte-offset bookkeeping. Anything other than zero invalidates the run's W
// data; it is not a dropped-record count.
func WAccountingErrors() uint64 { return http2.WAccountingErrors() }

// wCollectorLoop is the single consumer of wQueue. All JSON work for W lines
// happens here rather than on the socket write path, and the resulting line then
// takes the ordinary enqueue route, so the process still has exactly one
// goroutine writing to the log files.
func wCollectorLoop() {
	drain := func() {
		for {
			select {
			case ev := <-wQueue:
				logWFlushed(ev)
			default:
				return
			}
		}
	}
	for {
		select {
		case ev := <-wQueue:
			logWFlushed(ev)
		case done := <-wFlushReq:
			drain()
			close(done)
		}
	}
}

// logWFlushed renders one W event. It is a separate line rather than a field on
// the server request line because W happens strictly after that line has been
// emitted: the response headers are not flushed until the handler has returned,
// and making the handler wait for W would deadlock -- the flush is triggered by
// the handler returning.
//
// conn and dst carry the same values as the server request line, so the join on
// (dst, server_request_id) can be cross-checked rather than merely trusted.
func logWFlushed(ev http2.ResponseHeadersFlushedEvent) {
	outcome := "ok"
	if ev.Err {
		outcome = "write_error"
	}
	bp := getLine(320)
	b := *bp
	b = append(b, '{')
	b = appendKV(b, "event", "server_response_headers_flushed", true)
	b = appendKV(b, "src", "NaN", false)
	b = appendKV(b, "dst", srcNF, false)
	b = appendKVUint64(b, "server_request_id", ev.ServerRequestID, false)
	b = appendKV(b, "conn", ev.ConnID, false)
	b = appendKVInt(b, "stream_id", int(ev.StreamID), false)
	b = appendKVTime(b, "server_response_headers_flushed_time", ev.At, false)
	b = appendKVInt(b, "batch_size", int(ev.BatchSize), false)
	b = appendKV(b, "outcome", outcome, false)
	b = append(b, '}')
	*bp = b
	enqueue(kindHTTP, bp)
}

// Flush blocks until every record enqueued before this call has been written and
// fsync'd-to-buffer and the buffers flushed to the files. Call it from the NF's
// shutdown path (e.g. on SIGTERM) so the last records are not lost when the pod
// terminates. It is best-effort and returns promptly if logging is disabled.
func Flush() {
	if flushReq == nil {
		return
	}
	// TYcustom: drain the W collector first and WAIT for it, so that every event
	// it is holding has been turned into a record and enqueued before the writer
	// is asked to drain. Doing these two in the other order, or concurrently,
	// loses the tail: the writer's drain is non-blocking, so records the
	// collector enqueues a moment later are simply not seen.
	//
	// This assumes traffic has already stopped, per the collection procedure. It
	// guarantees "nothing already queued is lost", not "nothing can arrive
	// afterwards".
	if wFlushReq != nil {
		wDone := make(chan struct{})
		wFlushReq <- wDone
		<-wDone
	}

	done := make(chan struct{})
	flushReq <- done // writer loops on this channel, so this always completes
	<-done
}

func init() { Init() }

// envOr returns the value of env key or def if unset/empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// writerLoop is the single consumer. It owns both files exclusively, so writes
// never interleave. It batches by draining whatever is currently buffered
// before flushing, to amortize syscalls under load.
func writerLoop() {
	httpFile, httpW := openLog(envOr(envHTTPPath, defaultHTTPPath))
	dbFile, dbW := openLog(envOr(envDBPath, defaultDBPath))
	defer func() {
		if httpW != nil {
			_ = httpW.Flush()
		}
		if dbW != nil {
			_ = dbW.Flush()
		}
		if httpFile != nil {
			_ = httpFile.Close()
		}
		if dbFile != nil {
			_ = dbFile.Close()
		}
	}()

	flushTicker := time.NewTicker(200 * time.Millisecond)
	defer flushTicker.Stop()

	for {
		select {
		case rec, ok := <-queue:
			if !ok {
				return
			}
			writeRec(httpW, dbW, rec)
			// Drain anything already queued without blocking, to batch writes.
			drain := len(queue)
			for i := 0; i < drain; i++ {
				writeRec(httpW, dbW, <-queue)
			}
		case <-flushTicker.C:
			flush(httpW, dbW)
		case done := <-flushReq:
			// Drain everything currently queued, then flush, then signal.
			drainAll(httpW, dbW)
			flush(httpW, dbW)
			close(done)
		}
	}
}

// drainAll writes every record currently buffered in the queue without blocking.
func drainAll(httpW, dbW *bufio.Writer) {
	for {
		select {
		case rec := <-queue:
			writeRec(httpW, dbW, rec)
		default:
			return
		}
	}
}

func flush(httpW, dbW *bufio.Writer) {
	if httpW != nil {
		_ = httpW.Flush()
	}
	if dbW != nil {
		_ = dbW.Flush()
	}
}

func openLog(path string) (*os.File, *bufio.Writer) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		// Logging must never crash the NF; fall back to dropping this stream.
		return nil, nil
	}
	return f, bufio.NewWriterSize(f, writerBufferSize)
}

func writeRec(httpW, dbW *bufio.Writer, rec record) {
	var w *bufio.Writer
	switch rec.kind {
	case kindHTTP:
		w = httpW
	case kindDB:
		w = dbW
	}
	if w != nil {
		_, _ = w.Write(*rec.line)
		_ = w.WriteByte('\n')
	}
	putLine(rec.line) // every path, including the one where the sink is disabled
}

// enqueue pushes a record without ever blocking the caller. If the queue is
// full the record is dropped (and counted) so the data plane is never stalled.
func enqueue(kind recKind, line *[]byte) {
	select {
	case queue <- record{kind: kind, line: line}:
	default:
		dropped.Add(1)
	}
}

// Dropped returns how many records were dropped due to a full queue. Useful to
// sanity-check that logging kept up with the offered load.
func Dropped() uint64 {
	return dropped.Load()
}

// lineRealloc counts client lines that outgrew httpLineCap. It lives outside the
// var block above so that adding it does not re-align that block's existing
// entries.
var lineRealloc atomic.Uint64

// LineReallocs returns how many client HTTP lines outgrew httpLineCap while
// being built. Read it next to Dropped(); anything other than zero means the
// capacity is mis-sized and those lines each paid an allocation plus a full-line
// memcpy on the synchronous path.
func LineReallocs() uint64 {
	return lineRealloc.Load()
}

// --- JSON line builders -----------------------------------------------------
//
// We build JSON by hand (no reflection / encoding/json) to keep the hot path
// allocation-light and fast. Field values are escaped for the small set of
// characters that can appear in URIs / collection names / ids.

func appendJSONString(b []byte, s string) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			b = append(b, '\\', '"')
		case '\\':
			b = append(b, '\\', '\\')
		case '\n':
			b = append(b, '\\', 'n')
		case '\r':
			b = append(b, '\\', 'r')
		case '\t':
			b = append(b, '\\', 't')
		default:
			if c < 0x20 {
				const hex = "0123456789abcdef"
				b = append(b, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			} else {
				b = append(b, c)
			}
		}
	}
	return append(b, '"')
}

// appendKey writes the `,"key":` prefix of a field.
//
// It deliberately does not go through appendJSONString. Every key used in this
// package is a compile-time constant drawn from [a-z0-9_], so escaping them
// means running a per-byte switch over roughly 120 characters per line to
// discover, every time, that nothing needs escaping. Copying them straight in is
// a single memmove.
//
// The invariant this relies on: keys are literals, never user or network data.
// If that ever stops being true, this must go back through appendJSONString.
func appendKey(b []byte, key string, first bool) []byte {
	if !first {
		b = append(b, ',')
	}
	b = append(b, '"')
	b = append(b, key...)
	return append(b, '"', ':')
}

func appendKV(b []byte, key, val string, first bool) []byte {
	b = appendKey(b, key, first)
	return appendJSONString(b, val)
}

// appendKVBool appends a boolean-valued JSON field. The value is emitted
// unquoted so downstream analysis reads it as a real boolean, not a string.
func appendKVBool(b []byte, key string, val bool, first bool) []byte {
	b = appendKey(b, key, first)
	if val {
		return append(b, "true"...)
	}
	return append(b, "false"...)
}

// appendKVInt appends an integer-valued JSON field. The value is emitted
// unquoted so downstream analysis reads it as a number, not a string.
func appendKVInt(b []byte, key string, val int, first bool) []byte {
	b = appendKey(b, key, first)
	return strconv.AppendInt(b, int64(val), 10)
}

// appendKVUint64 is appendKVInt for values that genuinely need the full uint64
// range, such as the monotonically increasing server request id.
func appendKVUint64(b []byte, key string, val uint64, first bool) []byte {
	b = appendKey(b, key, first)
	return strconv.AppendUint(b, val, 10)
}

// appendKVTime writes a timestamp field straight into b.
//
// This replaces appendKV(b, key, formatTime(t), first), which cost one heap
// allocation per timestamp (Time.Format builds the text in a stack buffer and
// then copies it to the heap with string(b)) plus a byte-by-byte escape scan of
// the result. AppendFormat writes into b directly, so both disappear.
//
// Two invariants make skipping appendJSONString safe, and both must hold if this
// is ever changed:
//
//	1. .UTC() is applied first, so the zone is always "Z" and never a name.
//	2. The layout is RFC3339Nano, whose output contains only 0-9 - : . T Z --
//	   nothing that JSON requires escaping.
//
// The zero time renders as "", matching what formatTimeOrEmpty did. Note this
// now also applies to fields that previously used formatTime and would have
// rendered a zero time as "0001-01-01T00:00:00Z"; those fields are always taken
// from time.Now() and are never zero in practice.
//
// The caller's buffer capacity matters more than it used to: AppendFormat writes
// into b, so an undersized b now costs a growslice that copies the whole line so
// far. The initial capacities below are sized from measured line lengths.
func appendKVTime(b []byte, key string, t time.Time, first bool) []byte {
	b = appendKey(b, key, first)
	b = append(b, '"')
	if !t.IsZero() {
		// t.UTC() returns a value and does not allocate. It does strip the
		// monotonic reading, which is exactly why it is applied here at append
		// time and never to a variable that is later used in a subtraction:
		// latency_us must keep using the monotonic clock.
		b = t.UTC().AppendFormat(b, time.RFC3339Nano)
	}
	return append(b, '"')
}

// LogHTTP records one outgoing HTTP request/response from this NF's view.
//   - dstNF:        destination NF name (best-effort, derived from URL host)
//   - method:       HTTP method
//   - uri:          full request URI
//   - ueID:         UE id this request is for (may be ""); used for requests
//     whose URI does not carry the UE id but whose body does
//   - connID:       the TCP connection this request went out on, as
//     "localIP:localPort". Empty when the transport never obtained a connection
//     (e.g. the dial itself failed), which is how a connection-level failure is
//     distinguished from a request that was actually sent.
//   - connSlot:     which round-robin transport slot carried this request
//     (0..connsPerPeer-1). This is the slot the request was ASSIGNED to, which
//     is what shows whether the round-robin split is even; connID says which
//     socket that slot happened to be holding at the time. The two differ when
//     a slot's connection dies and is replaced, so a slot can span several
//     connID values over a run.
//   - connReused:   true if an existing connection was reused, false if this
//     request is what caused a new connection to be established. Grouping the
//     false records by time shows when (and whether) the connection pool grew.
//   - streamID:     the HTTP/2 stream id this request was sent on. Together
//     with conn it identifies the request uniquely on the wire, which is what
//     lets a client line be joined exactly to the server line for the same
//     request instead of guessed at by UE id, URI and timestamp order. 0 means
//     no stream was ever allocated (the request failed earlier); such lines
//     must be excluded from the join.
//   - retryCount:   how many transport-level retries preceded the attempt this
//     line describes (0 = first and only attempt). On a retried request the
//     recorded timestamps come from different attempts -- wroteTime is the
//     first write, connID the last connection -- so only retry_count == 0 lines
//     are safe to analyse as one coherent request.
//   - reqTime:      when the request was handed to the transport
//   - headerMuStart: when this attempt began contending for the connection's
//     HTTP/2 request-header lock, i.e. the start of the transport's serialised
//     send path. It splits the existing reqTime->wroteTime interval into
//     "before reaching the send path" and "inside the send path". Zero if the
//     request failed before reaching it.
//   - headerMuAcq:   when this attempt actually took that lock. Together with
//     headerMuStart it isolates the pure reqHeaderMu queueing time, which the
//     single headerMuStart point could not separate from the work done inside
//     the lock. The wait is computed offline as headerMuAcq - headerMuStart; it
//     is deliberately not computed here, because that would add a second store
//     inside the lock's critical section. Zero when the lock was never taken --
//     note that headerMuStart set with this zero means the request was cancelled
//     WHILE queued for the lock, which is its own outcome and not a gap.
//   - wroteTime:    when every frame of the request had reached the kernel
//     socket buffer. Zero if the request failed before it was written.
//   - gotFirstByte: when the first byte of the response reached this process's
//     read loop. Zero if no response ever arrived.
//   - respTime:     when the response (or error) was received
//
// A zero wroteTime/gotFirstByte/headerMuStart/headerMuAcq is emitted as "" so the
// reader can skip it. latency_us keeps its original meaning, respTime - reqTime,
// so existing analysis scripts are unaffected. Existing field names and their
// order are unchanged; new fields are inserted in timestamp order rather than
// renaming anything.
func LogHTTP(dstNF, method, uri, ueID, connID string, connSlot int, connReused bool,
	streamID uint32, retryCount int,
	reqTime, headerMuStart, headerMuAcq, wroteTime, gotFirstByte, respTime time.Time,
) {
	// See httpLineCap above for how the capacity was measured; the check after the
	// appends verifies it was enough.
	bp := getLine(httpLineCap)
	b := *bp
	b = append(b, '{')
	b = appendKV(b, "src", srcNF, true)
	b = appendKV(b, "dst", dstNF, false)
	b = appendKV(b, "method", method, false)
	b = appendKV(b, "uri", uri, false)
	b = appendKV(b, "ue_id", ueID, false)
	b = appendKV(b, "conn", connID, false)
	b = appendKVInt(b, "conn_slot", connSlot, false)
	b = appendKVBool(b, "conn_reused", connReused, false)
	b = appendKVInt(b, "stream_id", int(streamID), false)
	b = appendKVInt(b, "retry_count", retryCount, false)
	b = appendKVTime(b, "req_time", reqTime, false)
	b = appendKVTime(b, "req_header_mu_start_time", headerMuStart, false)
	b = appendKVTime(b, "req_header_mu_acq_time", headerMuAcq, false)
	b = appendKVTime(b, "wrote_time", wroteTime, false)
	b = appendKVTime(b, "got_first_byte", gotFirstByte, false)
	b = appendKVTime(b, "resp_time", respTime, false)
	b = appendDurUs(b, "latency_us", respTime.Sub(reqTime))
	b = append(b, '}')
	// Compare against the constant, not against the buffer's starting capacity:
	// linePool is shared with the other record kinds (AMF's LogWorker asks for
	// 384+160*len(sbi), which can exceed this), so a line that overran
	// httpLineCap could silently fit in a larger recycled buffer and go
	// uncounted. This form has no false negatives.
	if len(b) > httpLineCap {
		lineRealloc.Add(1)
	}
	*bp = b
	enqueue(kindHTTP, bp)
}

// LogHTTPInbound records one *incoming* HTTP request/response from the
// receiver's (this NF's, the server's) point of view. It is the server-side
// counterpart of LogHTTP and is written to the SAME HTTP_log.txt with the SAME
// fields, so client-view and server-view lines can be analyzed together.
//
// The sender NF cannot be reliably identified from the server side (the SBI URI
// does not carry it, and direct communication carries no token), so "src" is
// recorded as the literal "NaN". "dst" is this NF (the receiver).
//   - method:    HTTP method
//   - uri:       request URI
//   - ueID:      UE id this request is for (may be ""); for requests whose URI
//     does not carry the UE id but whose body does
//   - connID:    the TCP connection this request arrived on, as
//     "clientIP:clientPort". Deliberately the same field name and the same
//     string the SENDER records as its own conn, so the two views join without
//     any field mapping.
//   - srvReqID:  process-local id for this inbound request. It joins this line
//     to the separate server_response_headers_flushed event for the same
//     response, which cannot be written on this line because it happens after
//     the handler has returned.
//   - streamID:  the HTTP/2 stream id. (conn, stream_id) is what joins this
//     line to the sender's line for the same request.
//   - handlerGo: when this request's handler goroutine was about to be started,
//     i.e. before Go scheduling, the gin middleware chain and everything else
//     that precedes reqTime below. It splits the sender's wrote_time -> reqTime
//     interval into "before the handler existed" and "after it was submitted".
//     Zero if the request did not come through an instrumented HTTP/2 server.
//   - reqTime:   when the request arrived at this server
//   - respTime:  when the response was sent back
func LogHTTPInbound(method, uri, ueID, connID string, srvReqID uint64, streamID uint32,
	handlerGo, reqTime, respTime time.Time,
) {
	// Measured server lines average 277 B and reach 320 B, before the four
	// fields added here.
	bp := getLine(512)
	b := *bp
	b = append(b, '{')
	b = appendKV(b, "src", "NaN", true)
	b = appendKV(b, "dst", srcNF, false)
	b = appendKV(b, "method", method, false)
	b = appendKV(b, "uri", uri, false)
	b = appendKV(b, "ue_id", ueID, false)
	b = appendKVUint64(b, "server_request_id", srvReqID, false)
	b = appendKV(b, "conn", connID, false)
	b = appendKVInt(b, "stream_id", int(streamID), false)
	b = appendKVTime(b, "server_handler_go_time", handlerGo, false)
	b = appendKVTime(b, "req_time", reqTime, false)
	b = appendKVTime(b, "resp_time", respTime, false)
	b = appendDurUs(b, "latency_us", respTime.Sub(reqTime))
	b = append(b, '}')
	*bp = b
	enqueue(kindHTTP, bp)
}

// LogDB records one NF<->MongoDB request/response from this NF's view.
//   - mongo:     mongodb endpoint/identifier
//   - resource:  collection / table name
//   - operation: the kind of DB operation performed (find/update/else, e.g.
//     "GetOne", "PutOne", "DeleteOne", ...) so each line states its op type
//   - ueID:      the UE id involved (may be empty)
//   - reqTime:   when the DB request was issued
//   - respTime:  when the DB reply was received
func LogDB(mongo, resource, operation, ueID string, reqTime, respTime time.Time) {
	// Measured DB lines average 253 B and reach 269 B, so 256 sat right on
	// the boundary and about half of them reallocated.
	bp := getLine(320)
	b := *bp
	b = append(b, '{')
	b = appendKV(b, "nf", srcNF, true)
	b = appendKV(b, "mongo", mongo, false)
	b = appendKV(b, "resource", resource, false)
	b = appendKV(b, "operation", operation, false)
	b = appendKV(b, "ue_id", ueID, false)
	b = appendKVTime(b, "req_time", reqTime, false)
	b = appendKVTime(b, "resp_time", respTime, false)
	b = appendDurUs(b, "latency_us", respTime.Sub(reqTime))
	b = append(b, '}')
	*bp = b
	enqueue(kindDB, bp)
}

func appendDurUs(b []byte, key string, d time.Duration) []byte {
	b = appendKey(b, key, false)
	return strconv.AppendInt(b, d.Microseconds(), 10)
}
