// Package control implements the master record file: a small, atomically-
// updated file that points to the last durable checkpoint.
//
// All file I/O goes through the VFD manager, consistent with the rest of
// the storage stack. The atomic-write protocol (write-temp + fsync +
// rename + fsync-dir) uses VFDCache.Rename and VFDCache.SyncDir.
package control

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

const (
	FileName = "control"
	TempName = "control.tmp"
	Magic    = 0xCAFEBABE
	Version  = 1
	Size     = 48
)

var (
	ErrNotFound = errors.New("control: file not found")
	ErrCorrupt  = errors.New("control: corrupt")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type Record struct {
	RedoLSN       wal.LSN
	CheckpointLSN wal.LSN
	NextTxID      common.TransactionID
}

// Read loads the control file from dir via the VFD manager.
func Read(vfd *disk.VFDCache, dir string) (*Record, error) {
	path := filepath.Join(dir, FileName)

	// GetOrOpen will fail with os.ErrNotExist if the file isn't there.
	// VFD's Open propagates the OS error, so we translate it to ErrNotFound.
	entry, err := vfd.GetOrOpen(disk.FileNode{Path: path}, os.O_RDWR)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("control: open %s: %w", path, err)
	}
	defer vfd.Release(entry)

	buf := make([]byte, Size)
	n, err := entry.ReadAt(buf, 0)
	if err != nil && n < Size {
		return nil, fmt.Errorf("%w: short read (%d bytes)", ErrCorrupt, n)
	}

	return decode(buf)
}

// Write atomically writes rec to dir via the VFD manager.
//
// Protocol:
//  1. Serialize with CRC.
//  2. Open control.tmp via VFD, write, fsync.
//  3. Close the temp entry.
//  4. VFD.Rename(control.tmp → control).
//  5. VFD.SyncDir(dir) so the rename survives a crash.
func Write(vfd *disk.VFDCache, dir string, rec *Record) error {
	if rec == nil {
		return fmt.Errorf("control: nil record")
	}

	buf := encode(rec)
	tmpPath := filepath.Join(dir, TempName)
	finalPath := filepath.Join(dir, FileName)

	// 2. Open, write, fsync the temp file.
	entry, err := vfd.GetOrOpen(disk.FileNode{Path: tmpPath}, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return fmt.Errorf("control: open tmp: %w", err)
	}

	if _, err := entry.WriteAt(buf, 0); err != nil {
		vfd.Release(entry)
		// Best-effort cleanup of the partially-written temp file.
		_ = vfd.RemoveUnlinked(tmpPath)
		return fmt.Errorf("control: write tmp: %w", err)
	}

	// fsync before rename — the data must be durable before the rename
	// makes it visible as the new control file.
	if err := entry.Sync(); err != nil {
		vfd.Release(entry)
		_ = vfd.RemoveUnlinked(tmpPath)
		return fmt.Errorf("control: fsync tmp: %w", err)
	}

	// 3. Release (drops the refcount; the entry stays cached but evictable).
	vfd.Release(entry)

	// 4. Atomic rename. VFD.Rename invalidates any open entries for
	//    either path so a subsequent Read sees the new file's contents.
	if err := vfd.Rename(tmpPath, finalPath); err != nil {
		_ = vfd.RemoveUnlinked(tmpPath)
		return fmt.Errorf("control: rename: %w", err)
	}

	// 5. fsync the directory. Without this, the rename is in the page cache
	//    and could be lost on a machine crash.
	if err := vfd.SyncDir(dir); err != nil {
		return fmt.Errorf("control: fsync dir: %w", err)
	}

	return nil
}

// Remove deletes the control file. Returns nil if already gone.
func Remove(vfd *disk.VFDCache, dir string) error {
	path := filepath.Join(dir, FileName)
	return vfd.RemoveUnlinked(path)
}

// encode serializes rec with a trailing CRC.
func encode(rec *Record) []byte {
	buf := make([]byte, Size)
	binary.LittleEndian.PutUint32(buf[0:], Magic)
	binary.LittleEndian.PutUint32(buf[4:], Version)
	binary.LittleEndian.PutUint64(buf[8:], uint64(rec.RedoLSN))
	binary.LittleEndian.PutUint64(buf[16:], uint64(rec.CheckpointLSN))
	binary.LittleEndian.PutUint64(buf[24:], uint64(rec.NextTxID))
	crc := crc32.Update(0, crcTable, buf[:Size])
	binary.LittleEndian.PutUint32(buf[36:], crc)
	return buf
}

// decode parses and validates buf.
func decode(buf []byte) (*Record, error) {
	if len(buf) != Size {
		return nil, fmt.Errorf("%w: size %d != %d", ErrCorrupt, len(buf), Size)
	}

	magic := binary.LittleEndian.Uint32(buf[0:])
	if magic != Magic {
		return nil, fmt.Errorf("%w: magic 0x%08X != 0x%08X",
			ErrCorrupt, magic, Magic)
	}

	version := binary.LittleEndian.Uint32(buf[4:])
	if version != Version {
		return nil, fmt.Errorf("%w: version %d != %d",
			ErrCorrupt, version, Version)
	}

	wantCRC := binary.LittleEndian.Uint32(buf[36:])
	crcBuf := make([]byte, Size)
	copy(crcBuf, buf)
	binary.LittleEndian.PutUint32(crcBuf[36:], 0)
	gotCRC := crc32.Update(0, crcTable, crcBuf)
	if gotCRC != wantCRC {
		return nil, fmt.Errorf("%w: CRC 0x%08X != 0x%08X",
			ErrCorrupt, gotCRC, wantCRC)
	}

	return &Record{
		RedoLSN:       wal.LSN(binary.LittleEndian.Uint64(buf[8:])),
		CheckpointLSN: wal.LSN(binary.LittleEndian.Uint64(buf[16:])),
		NextTxID:      common.TransactionID(binary.LittleEndian.Uint64(buf[24:])),
	}, nil
}
