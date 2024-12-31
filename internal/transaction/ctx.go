package transaction

import (
	"time"

	"github.com/google/uuid"
	"github.com/tobsdb/tobsdb/internal/builder"
)

type TransactionLog struct {
	startTime time.Time

	apply string
	undo  string
}

type TransactionCtx struct {
	t *builder.TobsDB

	id        uuid.UUID
	startTime time.Time
	Persisted bool

	log []*TransactionLog

	Data builder.TDBData
}

// TODO(Tobshub): ALL actions should use transactions that are immediately commited by default
// TODO(Tobshub):
// any applied actions get stored in a boneless schema.
// find/update requests check the boneless schema for rows first before checking the original schema
// this ensures new/edited rows are used in the transaction.
// on commit, data from the boneless schema is applied to the original schema
// on rollback, the boneless schema is simply discarded.
func NewTransactionCtx(t *builder.TobsDB) *TransactionCtx {
	return &TransactionCtx{t, uuid.Must(uuid.NewV7()), time.Now(), false, make([]*TransactionLog, 2), builder.TDBData{}}
}

func (ctx *TransactionCtx) Commit() error { return nil }

func (ctx *TransactionCtx) Rollback() error { return nil }
