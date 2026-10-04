package wal

import (
	"hash/crc32"

	"github.com/surajjagdev/ToyDB/internal/common"
)

// record for WAL to be recorded

type RecordType uint8

const (
	RecBegin RecordType = iota + 1
	RecCommit
	RecAbort
	RecInsert
	RecUpdate
	RecDelete
	RecFPW // full page write
	RecCheckpoint
	RecClear
)

// 8 total flags for recovery passes and undos
const (
	FlagHasBefore uint8 = 1 << 0 // = 0b0000_0001 = 1
	FlagHasAfter  uint8 = 1 << 1 // = 0b0000_0010 = 2
)

const (
	MaxPayload = 64 << 20
)

// offset 0:  TotalLen(4)
// offset 4:  LSN(8)
// offset 12: PrevLSN(8)
// offset 20: TxID(4)
// offset 24: Type(1)
// offset 25: Flags(1)
// offset 26: Reserved(2)  ← padding
// offset 28: CRC(4)
const RecordHeaderSize = 32

// Record represents a single durable entry in the WAL.
type Record struct {
	LSN     LSN                  // Assigned by the writer at append time
	PrevLSN LSN                  // The previous record of the same transaction (Undo Chain)
	TxID    common.TransactionID // Which txn wrote this
	Type    RecordType
	Flags   uint8
	Data    []byte
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// EncodedSize returns the total size of the record once serialized.
func (r *Record) EncodedSize() int {
	return RecordHeaderSize + len(r.Data)
}

func calculateChecksum(headerPrefix, payload []byte) uint32 {
	crc := crc32.Update(0, crcTable, headerPrefix)
	crc = crc32.Update(crc, crcTable, payload)
	return crc
}

// Serialize data to dst buffer and calc the checksum
func (r *Record) MarshalTo(dst []byte) {
	if len(dst) < r.EncodedSize() {
		panic("Dst buffer too small for payload to serialize to")
	}

	totalLen := uint32(r.EncodedSize())

	// write headers
	common.ByteOrder.PutUint32(dst[0:4], totalLen)
	common.ByteOrder.PutUint64(dst[4:12], uint64(r.LSN))
	common.ByteOrder.PutUint64(dst[12:20], uint64(r.PrevLSN))
	common.ByteOrder.PutUint32(dst[20:24], uint32(r.TxID))

	dst[24] = uint8(r.Type)
	dst[25] = r.Flags

	// 26 and 27 are padding (explicitly zeroed for deterministic CRC)
	dst[26] = 0
	dst[27] = 0

	// zero out crc and everything else before calculating
	common.ByteOrder.PutUint32(dst[28:], 0)

	checksum := calculateChecksum(dst[0:28], r.Data)

	common.ByteOrder.PutUint32(dst[28:32], checksum)

	// cp payload to data
	copy(dst[32:], r.Data)
}

// Read record from byte array
func (r *Record) Unmarshal(data []byte) (int, error) {
	if len(data) < RecordHeaderSize {
		return len(data), ErrShortRead
	}

	totalLen := common.ByteOrder.Uint32(data[0:4])
	dataLen := totalLen - RecordHeaderSize

	if totalLen < RecordHeaderSize || totalLen > RecordHeaderSize+MaxPayload {
		return 0, ErrCorrupt
	}
	if uint32(len(data)) < totalLen {
		return len(data), ErrShortRead
	}

	// verify checksum
	expectedCRC := common.ByteOrder.Uint32(data[28:32])
	calculatedCheckSum := calculateChecksum(data[0:28], data[32:totalLen])

	if calculatedCheckSum != expectedCRC {
		return 0, ErrCorrupt
	}

	// read the fields and populate them
	r.LSN = LSN(common.ByteOrder.Uint64(data[4:12]))
	r.PrevLSN = LSN(common.ByteOrder.Uint64(data[12:20]))
	r.TxID = common.TransactionID(common.ByteOrder.Uint32(data[20:24]))
	r.Type = RecordType(data[24])
	r.Flags = data[25]

	r.Data = make([]byte, dataLen)
	copy(r.Data, data[32:totalLen])

	return int(totalLen), nil
}

// Read total len of record on disk
func PeekRecordLen(buf []byte) (uint32, error) {
	if len(buf) < 4 {
		return 0, ErrShortRead
	}
	total := common.ByteOrder.Uint32(buf[0:4])
	if total < RecordHeaderSize || total > RecordHeaderSize+MaxPayload {
		return 0, ErrCorrupt
	}
	return total, nil
}

// -----------------------------------------------------------------------------
// Payload codecs (add per record type as you go)
// -----------------------------------------------------------------------------

func EncodeFPW(pageID uint64, page []byte) []byte {
	out := make([]byte, 8+len(page))
	common.ByteOrder.PutUint64(out[0:], pageID)
	copy(out[8:], page)
	return out
}

func DecodeFPW(b []byte) (pageID uint64, page []byte, err error) {
	if len(b) < 8 {
		return 0, nil, ErrCorrupt
	}
	return common.ByteOrder.Uint64(b[0:]), b[8:], nil
}

func EncodeInsert(pageID uint64, slot uint16, tuple []byte) []byte {
	out := make([]byte, 10+len(tuple))
	common.ByteOrder.PutUint64(out[0:], pageID)
	common.ByteOrder.PutUint16(out[8:], slot)
	copy(out[10:], tuple)
	return out
}

func DecodeInsert(b []byte) (pageID uint64, slot uint16, tuple []byte, err error) {
	if len(b) < 10 {
		return 0, 0, nil, ErrCorrupt
	}
	return common.ByteOrder.Uint64(b[0:]), common.ByteOrder.Uint16(b[8:]), b[10:], nil
}
