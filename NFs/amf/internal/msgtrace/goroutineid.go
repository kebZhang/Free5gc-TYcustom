package msgtrace

import (
	"runtime"
	"strconv"
)

// goroutineID parses the current goroutine's id from its stack header. It is a
// local copy of the same helper in the recvtime package (kept separate to avoid
// an import edge between the two leaf packages). It is called a few times per
// NAS message (Begin/SetID/AddSBI/End), so the cost is negligible relative to
// NAS decoding and SBI round trips on the same path.
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Header format: "goroutine <id> [running]:\n..."
	s := buf[:n]
	const prefix = "goroutine "
	s = s[len(prefix):]
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	id, _ := strconv.ParseUint(string(s[:i]), 10, 64)
	return id
}
