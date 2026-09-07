package access

import (
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
)

/*
*
CRUD for physical tuples
CREATE tuple -> insert tuple given tuple, schema, txn id, cmd id -> record id
-> serializes tuple, ask fsm for space
READ tuple ->

*
*/

type RecordId struct {
	BlockID common.BlockID
	Slot    uint16
}

type TableHeap struct {
	bp   *buffer.BufferPool
	fsm  *FSM
	rel  common.RelationID
	fork common.ForkID
}

func NewTableHeap(bp *buffer.BufferPool, fsm *FSM, rel common.RelationID) *TableHeap {
	return &TableHeap{
		bp:   bp,
		fsm:  fsm,
		rel:  rel,
		fork: common.ForkMain, // always main
	}
}

// func (th *TableHeap) InsertTuple()
