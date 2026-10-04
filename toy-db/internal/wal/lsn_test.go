package wal

import "testing"

func TestMakeLSN(t *testing.T) {
	tests := []struct {
		name      string
		segmentNo uint32
		offset    uint32
		want      LSN
	}{
		{"zero", 0, 0, LSN(0)},
		{"simple", 1, 2, LSN(0x00000001_00000002)},
		{"max segment", 0xFFFFFFFF, 0, LSN(0xFFFFFFFF_00000000)},
		{"max offset", 0, 0xFFFFFFFF, LSN(0x00000000_FFFFFFFF)},
		{"both max", 0xFFFFFFFF, 0xFFFFFFFF, InvalidLSN},
		{"max-1 offset", 0xFFFFFFFF, 0xFFFFFFFE, MaxLSN},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MakeLSN(tt.segmentNo, tt.offset)
			if got != tt.want {
				t.Errorf("MakeLSN(%d, %d) = %v, want %v", tt.segmentNo, tt.offset, got, tt.want)
			}
		})
	}
}

func TestGetSegmentNumber(t *testing.T) {
	tests := []struct {
		name string
		lsn  LSN
		want uint32
	}{
		{"zero", LSN(0), 0},
		{"simple", MakeLSN(1, 2), 1},
		{"max", MakeLSN(0xFFFFFFFF, 0), 0xFFFFFFFF},
		{"invalid", InvalidLSN, 0xFFFFFFFF},
		{"maxlsn", MaxLSN, 0xFFFFFFFF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.lsn.GetSegmentNumber()
			if got != tt.want {
				t.Errorf("LSN(%x).GetSegmentNumber() = %d, want %d", uint64(tt.lsn), got, tt.want)
			}
		})
	}
}

func TestGetOffset(t *testing.T) {
	tests := []struct {
		name string
		lsn  LSN
		want uint32
	}{
		{"zero", LSN(0), 0},
		{"simple", MakeLSN(1, 2), 2},
		{"max", MakeLSN(0, 0xFFFFFFFF), 0xFFFFFFFF},
		{"invalid", InvalidLSN, 0xFFFFFFFF},
		{"maxlsn", MaxLSN, 0xFFFFFFFE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.lsn.GetOffset()
			if got != tt.want {
				t.Errorf("LSN(%x).GetOffset() = %d, want %d", uint64(tt.lsn), got, tt.want)
			}
		})
	}
}

func TestIsZero(t *testing.T) {
	tests := []struct {
		name string
		lsn  LSN
		want bool
	}{
		{"zero", LSN(0), true},
		{"make zero", MakeLSN(0, 0), true},
		{"non-zero", MakeLSN(0, 1), false},
		{"non-zero segment", MakeLSN(1, 0), false},
		{"invalid", InvalidLSN, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.lsn.IsZero(); got != tt.want {
				t.Errorf("LSN(%x).IsZero() = %v, want %v", uint64(tt.lsn), got, tt.want)
			}
		})
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		name string
		lsn  LSN
		want string
	}{
		{"zero", LSN(0), "0/0"},
		{"simple", MakeLSN(1, 2), "1/2"},
		{"segment only", MakeLSN(0xABCD, 0), "ABCD/0"},
		{"offset only", MakeLSN(0, 0x1234), "0/1234"},
		{"max", InvalidLSN, "FFFFFFFF/FFFFFFFF"},
		{"maxlsn", MaxLSN, "FFFFFFFF/FFFFFFFE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.lsn.String(); got != tt.want {
				t.Errorf("LSN(%x).String() = %q, want %q", uint64(tt.lsn), got, tt.want)
			}
		})
	}
}

func TestConstants(t *testing.T) {
	if InvalidLSN != LSN(^uint64(0)) {
		t.Errorf("InvalidLSN = %x, want %x", uint64(InvalidLSN), ^uint64(0))
	}
	if MaxLSN != InvalidLSN-1 {
		t.Errorf("MaxLSN = %x, want %x", uint64(MaxLSN), uint64(InvalidLSN-1))
	}

	// Verify extraction on constants
	if seg := InvalidLSN.GetSegmentNumber(); seg != 0xFFFFFFFF {
		t.Errorf("InvalidLSN.GetSegmentNumber() = %x, want FFFFFFFF", seg)
	}
	if off := InvalidLSN.GetOffset(); off != 0xFFFFFFFF {
		t.Errorf("InvalidLSN.GetOffset() = %x, want FFFFFFFF", off)
	}
	if seg := MaxLSN.GetSegmentNumber(); seg != 0xFFFFFFFF {
		t.Errorf("MaxLSN.GetSegmentNumber() = %x, want FFFFFFFF", seg)
	}
	if off := MaxLSN.GetOffset(); off != 0xFFFFFFFE {
		t.Errorf("MaxLSN.GetOffset() = %x, want FFFFFFFE", off)
	}
}

func TestRoundTrip(t *testing.T) {
	segments := []uint32{0, 1, 0x12345678, 0xFFFFFFFF}
	offsets := []uint32{0, 1, 0x87654321, 0xFFFFFFFF}
	for _, seg := range segments {
		for _, off := range offsets {
			lsn := MakeLSN(seg, off)
			if gotSeg := lsn.GetSegmentNumber(); gotSeg != seg {
				t.Errorf("MakeLSN(%d, %d).GetSegmentNumber() = %d, want %d", seg, off, gotSeg, seg)
			}
			if gotOff := lsn.GetOffset(); gotOff != off {
				t.Errorf("MakeLSN(%d, %d).GetOffset() = %d, want %d", seg, off, gotOff, off)
			}
		}
	}
}
