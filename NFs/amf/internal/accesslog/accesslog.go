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
)

// srcNF is the name of the NF this binary runs as (the requester for HTTP logs,
// and the "NF" side for DB logs). Set once at package init.
const srcNF = "AMF"

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
	line []byte
}

const (
	// queueCapacity bounds memory use. With large-memory experiment hosts this
	// is sized to absorb sustained bursts before falling back to drop-on-full.
	queueCapacity = 1 << 21 // 2097152
	writerBufferSize = 1 << 20 // 1 MiB

	envHTTPPath = "HTTP_LOG_PATH"
	envDBPath   = "DB_LOG_PATH"

	defaultHTTPPath = "/tmp/HTTP_log.txt"
	defaultDBPath   = "/tmp/DB_log.txt"
)

var (
	queue    chan record
	dropped  atomic.Uint64      // count of records dropped because the queue was full
	initOne  sync.Once
	flushReq chan chan struct{} // request a synchronous flush from the writer
)

// Init starts the background writer. It is safe to call multiple times; only the
// first call has any effect. It is invoked automatically on first use, but NFs
// may call it explicitly at startup.
func Init() {
	initOne.Do(func() {
		queue = make(chan record, queueCapacity)
		flushReq = make(chan chan struct{})
		go writerLoop()
	})
}

// Flush blocks until every record enqueued before this call has been written and
// fsync'd-to-buffer and the buffers flushed to the files. Call it from the NF's
// shutdown path (e.g. on SIGTERM) so the last records are not lost when the pod
// terminates. It is best-effort and returns promptly if logging is disabled.
func Flush() {
	if flushReq == nil {
		return
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
	if w == nil {
		return
	}
	_, _ = w.Write(rec.line)
	_ = w.WriteByte('\n')
}

// enqueue pushes a record without ever blocking the caller. If the queue is
// full the record is dropped (and counted) so the data plane is never stalled.
func enqueue(kind recKind, line []byte) {
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

func appendKV(b []byte, key, val string, first bool) []byte {
	if !first {
		b = append(b, ',')
	}
	b = appendJSONString(b, key)
	b = append(b, ':')
	b = appendJSONString(b, val)
	return b
}

// formatTime renders a timestamp as RFC3339Nano (UTC) for stable sorting.
func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// LogHTTP records one outgoing HTTP request/response from this NF's view.
//   - dstNF:    destination NF name (best-effort, derived from URL host)
//   - method:   HTTP method
//   - uri:      full request URI
//   - ueID:     UE id this request is for (may be ""); used for requests whose
//     URI does not carry the UE id but whose body does
//   - reqTime:  when the request was sent
//   - respTime: when the response (or error) was received
func LogHTTP(dstNF, method, uri, ueID string, reqTime, respTime time.Time) {
	b := make([]byte, 0, 256)
	b = append(b, '{')
	b = appendKV(b, "src", srcNF, true)
	b = appendKV(b, "dst", dstNF, false)
	b = appendKV(b, "method", method, false)
	b = appendKV(b, "uri", uri, false)
	b = appendKV(b, "ue_id", ueID, false)
	b = appendKV(b, "req_time", formatTime(reqTime), false)
	b = appendKV(b, "resp_time", formatTime(respTime), false)
	b = appendDurUs(b, "latency_us", respTime.Sub(reqTime))
	b = append(b, '}')
	enqueue(kindHTTP, b)
}

// LogDB records one NF<->MongoDB request/response from this NF's view.
//   - mongo:    mongodb endpoint/identifier
//   - resource: collection / table name
//   - ueID:     the UE id involved (may be empty)
//   - reqTime:  when the DB request was issued
//   - respTime: when the DB reply was received
func LogDB(mongo, resource, ueID string, reqTime, respTime time.Time) {
	b := make([]byte, 0, 256)
	b = append(b, '{')
	b = appendKV(b, "nf", srcNF, true)
	b = appendKV(b, "mongo", mongo, false)
	b = appendKV(b, "resource", resource, false)
	b = appendKV(b, "ue_id", ueID, false)
	b = appendKV(b, "req_time", formatTime(reqTime), false)
	b = appendKV(b, "resp_time", formatTime(respTime), false)
	b = appendDurUs(b, "latency_us", respTime.Sub(reqTime))
	b = append(b, '}')
	enqueue(kindDB, b)
}

func appendDurUs(b []byte, key string, d time.Duration) []byte {
	b = append(b, ',')
	b = appendJSONString(b, key)
	b = append(b, ':')
	b = strconv.AppendInt(b, d.Microseconds(), 10)
	return b
}
