package transaction

import (
	"time"

	"github.com/google/uuid"
	"github.com/tobsdb/tobsdb/internal/builder"
)

type TransactionCtx struct {
	Schema *builder.Schema
	id     uuid.UUID

	startTime time.Time
	Persisted bool
}

// TODO(Tobshub): ALL actions should use transactions that are immediately commited by default
// TODO(Tobshub):
// any applied actions get stored in a boneless schema.
// find/update requests check the boneless schema for rows first before checking the original schema
// this ensures new/edited rows are used in the transaction.
// on commit, data from the boneless schema is applied to the original schema
// on rollback, the boneless schema is simply discarded.
func NewTransactionCtx(s *builder.Schema) *TransactionCtx {
	return &TransactionCtx{s.NewSnapshot(), uuid.Must(uuid.NewV7()), time.Now(), false}
}

func (ctx *TransactionCtx) Commit(s *builder.Schema) error {
	return s.ApplySnapshot(ctx.Schema)
}

func (ctx *TransactionCtx) Rollback() error { return nil }
