package wal

import (
	"bytes"
	"hash/crc32"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
)

// --- helpers ---

func sampleRecord() *Record {
	return &Record{
		LSN:     MakeLSN(3, 42),
		PrevLSN: MakeLSN(1, 7),
		TxID:    common.TransactionID(99),
		Type:    RecInsert,
		Flags:   FlagHasAfter,
		Data:    []byte("hello world"),
	}
}

// recomputeCRC mirrors production's calculateChecksum so tests can verify
// the stored CRC independently. Must use the same polynomial as production.
var testCRCTable = crc32.MakeTable(crc32.Castagnoli)

func recomputeCRC(header []byte, payload []byte) uint32 {
	crc := crc32.Update(0, testCRCTable, header)
	crc = crc32.Update(crc, testCRCTable, payload)
	return crc
}

// --- EncodedSize ---

func TestRecordEncodedSize(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want int
	}{
		{"nil data", nil, RecordHeaderSize},
		{"empty data", []byte{}, RecordHeaderSize},
		{"small", []byte("abc"), RecordHeaderSize + 3},
		{"large", make([]byte, 1024), RecordHeaderSize + 1024},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Record{Data: tt.data}
			if got := r.EncodedSize(); got != tt.want {
				t.Errorf("EncodedSize() = %d, want %d", got, tt.want)
			}
		})
	}
}

// --- Marshal field layout ---

func TestMarshalToHeaderLayout(t *testing.T) {
	r := sampleRecord()
	dst := make([]byte, r.EncodedSize())
	r.MarshalTo(dst)

	if got := common.ByteOrder.Uint32(dst[0:4]); got != uint32(r.EncodedSize()) {
		t.Errorf("TotalLen = %d, want %d", got, r.EncodedSize())
	}
	if got := LSN(common.ByteOrder.Uint64(dst[4:12])); got != r.LSN {
		t.Errorf("LSN = %x, want %x", got, r.LSN)
	}
	if got := LSN(common.ByteOrder.Uint64(dst[12:20])); got != r.PrevLSN {
		t.Errorf("PrevLSN = %x, want %x", got, r.PrevLSN)
	}
	if got := common.TransactionID(common.ByteOrder.Uint32(dst[20:24])); got != r.TxID {
		t.Errorf("TxID = %d, want %d", got, r.TxID)
	}
	if got := RecordType(dst[24]); got != r.Type {
		t.Errorf("Type = %d, want %d", got, r.Type)
	}
	if got := dst[25]; got != r.Flags {
		t.Errorf("Flags = %d, want %d", got, r.Flags)
	}
	if dst[26] != 0 || dst[27] != 0 {
		t.Errorf("reserved bytes not zeroed: %d %d", dst[26], dst[27])
	}
	if !bytes.Equal(dst[32:], r.Data) {
		t.Errorf("payload mismatch: got %q, want %q", dst[32:], r.Data)
	}
}

func TestMarshalToCRCIsCorrect(t *testing.T) {
	r := sampleRecord()
	dst := make([]byte, r.EncodedSize())
	r.MarshalTo(dst)

	stored := common.ByteOrder.Uint32(dst[28:32])
	want := recomputeCRC(dst[0:28], r.Data)
	if stored != want {
		t.Errorf("CRC = %x, want %x", stored, want)
	}
}

func TestMarshalToDeterministic(t *testing.T) {
	r := sampleRecord()
	a := make([]byte, r.EncodedSize())
	b := make([]byte, r.EncodedSize())
	r.MarshalTo(a)
	r.MarshalTo(b)
	if !bytes.Equal(a, b) {
		t.Errorf("MarshalTo is not deterministic")
	}
}

// TestMarshalToOverwritesReservedBytes verifies that reserved bytes are
// zeroed even when the destination buffer already holds non-zero data.
// Without this, a reused buffer would produce a CRC over stale bytes.
func TestMarshalToOverwritesReservedBytes(t *testing.T) {
	r := sampleRecord()
	dst := make([]byte, r.EncodedSize())
	// Poison the reserved and CRC slots.
	dst[26] = 0xAA
	dst[27] = 0xBB
	dst[28] = 0xCC
	dst[29] = 0xDD
	dst[30] = 0xEE
	dst[31] = 0xFF

	r.MarshalTo(dst)

	if dst[26] != 0 || dst[27] != 0 {
		t.Errorf("reserved bytes not zeroed: %d %d", dst[26], dst[27])
	}
	// CRC must match the freshly computed value over the zeroed header.
	want := recomputeCRC(dst[0:28], r.Data)
	got := common.ByteOrder.Uint32(dst[28:32])
	if got != want {
		t.Errorf("CRC = %x, want %x", got, want)
	}
}

func TestMarshalToReusedBuffer(t *testing.T) {
	r1 := sampleRecord()
	r2 := sampleRecord()
	r2.LSN = MakeLSN(9, 9)
	r2.Type = RecDelete
	r2.Flags = 0
	r2.Data = []byte("x")

	buf := make([]byte, r1.EncodedSize())
	r1.MarshalTo(buf)
	// Marshal a smaller record into the same underlying array.
	r2.MarshalTo(buf[:r2.EncodedSize()])

	if got := common.ByteOrder.Uint32(buf[0:4]); got != uint32(r2.EncodedSize()) {
		t.Errorf("TotalLen after reuse = %d, want %d", got, r2.EncodedSize())
	}
	stored := common.ByteOrder.Uint32(buf[28:32])
	want := recomputeCRC(buf[0:28], r2.Data)
	if stored != want {
		t.Errorf("CRC after reuse = %x, want %x", stored, want)
	}
}

func TestMarshalToPanicsOnSmallBuffer(t *testing.T) {
	r := sampleRecord()
	dst := make([]byte, r.EncodedSize()-1)
	defer func() {
		if recover() == nil {
			t.Errorf("expected panic for small buffer")
		}
	}()
	r.MarshalTo(dst)
}

// --- Round trip ---

func TestRecordRoundTrip(t *testing.T) {
	types := []RecordType{
		RecBegin, RecCommit, RecAbort, RecInsert, RecUpdate,
		RecDelete, RecFPW, RecCheckpoint, RecClear,
	}
	payloads := [][]byte{
		nil, {}, []byte("a"), []byte("hello world"),
		bytes.Repeat([]byte{0xAB}, 1000),
	}

	for _, typ := range types {
		for i, payload := range payloads {
			r := &Record{
				LSN:     MakeLSN(uint32(typ), uint32(i)),
				PrevLSN: MakeLSN(1, 1),
				TxID:    common.TransactionID(42),
				Type:    typ,
				Flags:   FlagHasBefore | FlagHasAfter,
				Data:    payload,
			}
			buf := make([]byte, r.EncodedSize())
			r.MarshalTo(buf)

			var out Record
			n, err := out.Unmarshal(buf)
			if err != nil {
				t.Fatalf("type=%d payload=%d: Unmarshal error: %v", typ, i, err)
			}
			if n != r.EncodedSize() {
				t.Errorf("type=%d payload=%d: n=%d, want %d", typ, i, n, r.EncodedSize())
			}
			if out.LSN != r.LSN || out.PrevLSN != r.PrevLSN || out.TxID != r.TxID ||
				out.Type != r.Type || out.Flags != r.Flags {
				t.Errorf("type=%d payload=%d: header mismatch: %+v vs %+v", typ, i, &out, r)
			}
			if !bytes.Equal(out.Data, r.Data) {
				t.Errorf("type=%d payload=%d: data mismatch: %q vs %q", typ, i, out.Data, r.Data)
			}
		}
	}
}

func TestUnmarshalTrailingBytesIgnored(t *testing.T) {
	r := sampleRecord()
	buf := make([]byte, r.EncodedSize()+16)
	r.MarshalTo(buf)

	var out Record
	n, err := out.Unmarshal(buf)
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if n != r.EncodedSize() {
		t.Errorf("n = %d, want %d", n, r.EncodedSize())
	}
}

// --- Failure cases ---

func TestUnmarshalShortHeader(t *testing.T) {
	for _, n := range []int{0, 1, 4, 20, RecordHeaderSize - 1} {
		buf := make([]byte, n)
		var r Record
		gotN, err := r.Unmarshal(buf)
		if err != ErrShortRead {
			t.Errorf("len=%d: err = %v, want ErrShortRead", n, err)
		}
		if gotN != n {
			t.Errorf("len=%d: n = %d, want %d", n, gotN, n)
		}
	}
}

func TestUnmarshalShortBody(t *testing.T) {
	r := sampleRecord()
	buf := make([]byte, r.EncodedSize())
	r.MarshalTo(buf)

	truncated := buf[:len(buf)-1]
	var out Record
	gotN, err := out.Unmarshal(truncated)
	if err != ErrShortRead {
		t.Errorf("err = %v, want ErrShortRead", err)
	}
	if gotN != len(truncated) {
		t.Errorf("n = %d, want %d", gotN, len(truncated))
	}
}

func TestUnmarshalCorruptChecksum(t *testing.T) {
	r := sampleRecord()
	buf := make([]byte, r.EncodedSize())
	r.MarshalTo(buf)

	buf[32] ^= 0xFF // flip a payload byte after CRC was computed

	var out Record
	if _, err := out.Unmarshal(buf); err != ErrCorrupt {
		t.Errorf("err = %v, want ErrCorrupt", err)
	}
}

func TestUnmarshalCorruptHeaderDetectedByCRC(t *testing.T) {
	r := sampleRecord()
	buf := make([]byte, r.EncodedSize())
	r.MarshalTo(buf)

	buf[4] ^= 0xFF // flip an LSN byte; CRC must catch it

	var out Record
	if _, err := out.Unmarshal(buf); err != ErrCorrupt {
		t.Errorf("err = %v, want ErrCorrupt", err)
	}
}

// TestUnmarshalPayloadTooLarge exercises the length-bounds check that runs
// before the CRC check. A 32-byte buffer with a bogus TotalLen is enough;
// production rejects the header before reading the (nonexistent) payload.
func TestUnmarshalPayloadTooLarge(t *testing.T) {
	buf := make([]byte, RecordHeaderSize)

	// Claim a payload one byte over MaxPayload.
	overLimit := uint32(RecordHeaderSize + MaxPayload + 1)
	common.ByteOrder.PutUint32(buf[0:4], overLimit)

	var out Record
	if _, err := out.Unmarshal(buf); err != ErrCorrupt {
		t.Errorf("err = %v, want ErrCorrupt", err)
	}
}

// TestUnmarshalPayloadTooSmall rejects a TotalLen smaller than the header.
func TestUnmarshalPayloadTooSmall(t *testing.T) {
	buf := make([]byte, RecordHeaderSize)
	common.ByteOrder.PutUint32(buf[0:4], uint32(RecordHeaderSize-1))

	var out Record
	if _, err := out.Unmarshal(buf); err != ErrCorrupt {
		t.Errorf("err = %v, want ErrCorrupt", err)
	}
}

// --- Edge / boundary ---

func TestUnmarshalZeroPayload(t *testing.T) {
	r := &Record{LSN: 1, Type: RecBegin, Data: nil}
	buf := make([]byte, r.EncodedSize())
	r.MarshalTo(buf)

	var out Record
	n, err := out.Unmarshal(buf)
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if n != RecordHeaderSize {
		t.Errorf("n = %d, want %d", n, RecordHeaderSize)
	}
	if len(out.Data) != 0 {
		t.Errorf("len(Data) = %d, want 0", len(out.Data))
	}
}

func TestUnmarshalMaxPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 64 MiB payload test in short mode")
	}
	payload := make([]byte, MaxPayload)
	for i := range payload {
		payload[i] = byte(i)
	}
	r := &Record{LSN: 1, Type: RecFPW, Data: payload}
	buf := make([]byte, r.EncodedSize())
	r.MarshalTo(buf)

	var out Record
	if _, err := out.Unmarshal(buf); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if !bytes.Equal(out.Data, payload) {
		t.Errorf("payload mismatch")
	}
}

func TestUnmarshalAliasesInput(t *testing.T) {
	r := sampleRecord()
	buf := make([]byte, r.EncodedSize())
	r.MarshalTo(buf)

	var out Record
	if _, err := out.Unmarshal(buf); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	original := append([]byte(nil), out.Data...)
	for i := range buf {
		buf[i] = 0xFF
	}
	if !bytes.Equal(out.Data, original) {
		t.Errorf("out.Data aliases input buffer")
	}
}

// TestUnmarshalReusesExistingData verifies that reusing the same Record for
// a second Unmarshal produces the correct payload. This catches buffer
// mishandling when a caller decodes a sequence of records into the same value.
func TestUnmarshalReusesExistingData(t *testing.T) {
	first := sampleRecord()
	buf1 := make([]byte, first.EncodedSize())
	first.MarshalTo(buf1)

	var r Record
	if _, err := r.Unmarshal(buf1); err != nil {
		t.Fatalf("first Unmarshal: %v", err)
	}

	second := &Record{
		LSN: MakeLSN(5, 5), Type: RecDelete,
		Data: []byte("second payload"),
	}
	buf2 := make([]byte, second.EncodedSize())
	second.MarshalTo(buf2)

	if _, err := r.Unmarshal(buf2); err != nil {
		t.Fatalf("second Unmarshal: %v", err)
	}
	if !bytes.Equal(r.Data, second.Data) {
		t.Errorf("second payload mismatch: got %q want %q", r.Data, second.Data)
	}
	if r.Type != RecDelete {
		t.Errorf("Type = %v, want RecDelete", r.Type)
	}
}

// --- RecordType sanity ---

func TestRecordTypeValues(t *testing.T) {
	want := []RecordType{
		RecBegin, RecCommit, RecAbort, RecInsert, RecUpdate,
		RecDelete, RecFPW, RecCheckpoint, RecClear,
	}
	for i, w := range want {
		if uint8(w) != uint8(i+1) {
			t.Errorf("RecordType %d = %d, want %d", i, w, i+1)
		}
	}
}

func TestFlagValues(t *testing.T) {
	if FlagHasBefore != 1 {
		t.Errorf("FlagHasBefore = %d, want 1", FlagHasBefore)
	}
	if FlagHasAfter != 2 {
		t.Errorf("FlagHasAfter = %d, want 2", FlagHasAfter)
	}
	if FlagHasBefore&FlagHasAfter != 0 {
		t.Errorf("flags must be distinct bits")
	}
}
