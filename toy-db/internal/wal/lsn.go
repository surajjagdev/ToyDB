package wal

import "fmt"

type LSN uint64

const (
	InvalidLSN LSN = LSN(^uint64(0))
	MaxLSN     LSN = InvalidLSN - 1
)

// Create the logsequence number -> common

// Create log sequence number using the segment of file and its offset inn file
func MakeLSN(segmentNo uint32, offset uint32) LSN {
	return (LSN(segmentNo) << 32) | LSN(offset)
}

func (lsn LSN) GetSegmentNumber() uint32 {
	return uint32(lsn >> 32)
}

func (lsn LSN) GetOffset() uint32 {
	return uint32(lsn)
}

// IsZero returns true if the LSN is the sentinel "no LSN" value.
func (l LSN) IsZero() bool {
	return l == 0
}

// String formats the LSN into the standard style
func (l LSN) String() string {
	if l.IsZero() {
		return "0/0"
	}

	return fmt.Sprintf("%X/%X", l.GetSegmentNumber(), l.GetOffset())
}
