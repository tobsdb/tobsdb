package conn

import (
	"errors"
	"net"
	"time"

	"github.com/tobsdb/tobsdb/internal/auth"
	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/transaction"
	"github.com/tobsdb/tobsdb/pkg"
)

type ConnCtx struct {
	conn        net.Conn
	attempts    int
	isAuthed    bool
	shouldClose bool

	User   *auth.TdbUser
	Schema *builder.Schema

	TxCtx *transaction.TransactionCtx
}

// New connections have a 30 second deadline.
// If the deadline is reached, and the connection is not authenticated, the connection is closed.
func NewConnCtx(c net.Conn) *ConnCtx {
	c.SetDeadline(time.Now().Add(30 * time.Second))
	return &ConnCtx{c, 0, false, false, nil, nil, nil}
}

// SetAuthed marks the connection as authenticated and removes the deadline.
func (ctx *ConnCtx) SetAuthed() {
	ctx.isAuthed = true
	ctx.conn.SetDeadline(time.Time{})
}

const (
	maxConnAttempts  = 3
	shouldCloseError = "connection no no wanna"
)

func (c *ConnCtx) Read() ([]byte, error) {
	if c.shouldClose {
		return nil, errors.New(shouldCloseError)
	}
	return pkg.ConnReadBytes(c.conn)
}

func (ctx *ConnCtx) Write(buf []byte) (int, error) {
	if ctx.shouldClose {
		return 0, errors.New(shouldCloseError)
	}
	return pkg.ConnWriteBytes(ctx.conn, buf)
}
func (ctx *ConnCtx) WriteString(buf string) (int, error)   { return ctx.Write([]byte(buf)) }
func (ctx *ConnCtx) WriteResponse(r Response) (int, error) { return ctx.Write(r.Marshal()) }
