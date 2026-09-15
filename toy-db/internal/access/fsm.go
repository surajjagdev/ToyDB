/**
We will use this to find which fsm pages hold info for current page.

Since each page is a byte, each fsm page has about 8192 - header (32) = 8160 / 2
=> can hold 4080 nodes. Half ~ internal. Max leaf nodes are 2040 leafs


**/

package access

import (
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/page/fsm"
)

type FSM struct {
	bp *buffer.BufferPool
}

func NewFSM(bp *buffer.BufferPool) *FSM {
	return &FSM{bp: bp}
}

// get offset
func getFSMOffset(tableBlock common.BlockID) (fsmBlock common.BlockID, leafOffset int) {
	fsmBlock = tableBlock / common.BlockID(fsm.LeavesPerPage)
	leafOffset = int(tableBlock) % fsm.LeavesPerPage

	return fsmBlock, leafOffset
}

// FSM accessor should be able to record free space
func (f *FSM) RecordFreeSpace(rel common.RelationID, tableBlock common.BlockID, freeBytes int) error {
	category := fsm.FreeBytesToCategory(freeBytes)

	fsmBlock, leafOffset := getFSMOffset(tableBlock)

	tag := buffer.BufferTag{
		RelationID: rel,
		ForkID:     common.ForkFSM,
		BlockID:    fsmBlock,
	}

	var frame *buffer.Frame
	var err error

	for {
		// get the frame from buffer pool
		frame, err, _ = f.bp.GetPage(tag)

		if err == nil {
			break
		}

		dm, dmErr := f.bp.GetDiskManager()
		if dmErr == nil {
			_, err = dm.AllocateBlock(rel, common.ForkFSM)

			if err != nil {
				break
			}
		}

		// fsm now physically exists, put into buffer pool
		frame, err = f.bp.AllocatePage(tag)

		if err == nil {
			// WE successfully allocated it! We must initialize it.
			frame.WLatch()
			fsm.InitFSMPage(frame.Page)
			frame.WUnlatch()
			break
		}

		// If AllocatePage failed, it means another thread JUST allocated it!
		// The loop will restart and successfully fetch it via GetPage.
	}

	frame.WLatch()
	fsmPage := &fsm.FSMPage{Page: frame.Page}
	fsmPage.UpdateAvailable(leafOffset, category)
	frame.WUnlatch()

	// set dirty
	frame.SetDirty()
	frame.Unpin()

	return nil
}

/*
*
Given the relation id, find a block in there to insert a new row/whatever
If nothing found,
*
*/
func (f *FSM) GetBlockWithFreeSpace(rel common.RelationID, freeBytesRequired int) (common.BlockID, error) {
	requiredCategory := fsm.FreeBytesToCategory(freeBytesRequired)

	// If no bytes a required for this insert, then return an invalid block id
	if requiredCategory == 0 {
		return common.InvalidBlockID, nil
	}

	var fsmBlock common.BlockID = 0

	for {
		tag := buffer.BufferTag{
			RelationID: rel,
			ForkID:     common.ForkFSM,
			BlockID:    fsmBlock,
		}

		frame, err, _ := f.bp.GetPage(tag)

		if err != nil {
			// End of all fsm blocks -> no space,
			// or none present
			// return no err
			return common.InvalidBlockID, nil
		}

		frame.RLatch()

		fsmPage := fsm.FSMPage{
			Page: frame.Page,
		}

		if fsmPage.GetMaxAvailable() >= requiredCategory {
			leafOffset := fsmPage.SearchAvailable(requiredCategory)
			frame.RUnlatch()
			frame.Unpin()

			if leafOffset != -1 {
				tableBlock := (fsmBlock * common.BlockID(fsm.LeavesPerPage)) + common.BlockID(leafOffset)
				return tableBlock, nil
			}
		} else {
			frame.RUnlatch()
			frame.Unpin()
		}

		fsmBlock++
	}
}
