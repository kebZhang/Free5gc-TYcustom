// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import "testing"

// TYcustom: guards the readFrameCh capacity set in serveConn. See
// Readframech_buffer1_0930.md at the repo root.
// [cap0 1120] capacity is back to the upstream 0; the capacity-1 variant
// wanted 1 here. Change wantReadFrameChCap together with server.go.
const wantReadFrameChCap = 0

func TestTYcustomReadFrameChCap(t *testing.T) { synctestTest(t, testTYcustomReadFrameChCap) }
func testTYcustomReadFrameChCap(t testing.TB) {
	st := newServerTester(t, nil)
	if got := cap(st.sc.readFrameCh); got != wantReadFrameChCap {
		t.Fatalf("cap(readFrameCh) = %d, want %d", got, wantReadFrameChCap)
	}
}
