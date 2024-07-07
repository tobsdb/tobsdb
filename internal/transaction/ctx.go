package transaction

type TransactionCtx struct{}

func NewTransactionCtx() *TransactionCtx {
	return &TransactionCtx{}
}

func (ctx *TransactionCtx) Commit() error { return nil }

func (ctx *TransactionCtx) Rollback() error { return nil }
