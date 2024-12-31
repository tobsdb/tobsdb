package conn

import (
	"fmt"
	"net/http"

	"github.com/tobsdb/tobsdb/internal/auth"
	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/transaction"
)

type RequestAction string

const (
	// rows actions
	RequestActionCreate     RequestAction = "create"
	RequestActionCreateMany RequestAction = "createMany"
	RequestActionFind       RequestAction = "findUnique"
	RequestActionFindMany   RequestAction = "findMany"
	RequestActionDelete     RequestAction = "deleteUnique"
	RequestActionDeleteMany RequestAction = "deleteMany"
	RequestActionUpdate     RequestAction = "updateUnique"
	RequestActionUpdateMany RequestAction = "updateMany"

	// database actions
	RequestActionCreateDB RequestAction = "createDatabase"
	RequestActionUseDB    RequestAction = "useDatabase"
	RequestActionDropDB   RequestAction = "dropDatabase"
	RequestActionListDB   RequestAction = "listDatabases"
	RequestActionDBStat   RequestAction = "databaseStats"

	// table actions
	RequestActionDropTable RequestAction = "dropTable"
	RequestActionMigration RequestAction = "migration"

	// user actions
	RequestActionCreateUser     RequestAction = "createUser"
	RequestActionDeleteUser     RequestAction = "deleteUser"
	RequestActionUpdateUserRole RequestAction = "updateUserRole"

	// TODO: transaction actions
	RequestActionTransaction RequestAction = "transaction"
	RequestActionCommit      RequestAction = "commit"
	RequestActionRollback    RequestAction = "rollback"
)

func (action RequestAction) IsReadOnly() bool {
	return action == RequestActionFind || action == RequestActionFindMany ||
		action == RequestActionDBStat || action == RequestActionListDB || action == RequestActionUseDB
}

func (action RequestAction) IsDBAction() bool {
	switch action {
	default:
		return false
	case RequestActionCreateDB, RequestActionDropDB, RequestActionListDB,
		RequestActionDBStat, RequestActionDropTable, RequestActionCreateUser, RequestActionDeleteUser,
		RequestActionUpdateUserRole:
		return true
	}
}

func ActionHandler(tdb *builder.TobsDB, action RequestAction, ctx *ConnCtx, raw []byte) Response {
	if action.IsReadOnly() {
		if !ctx.Schema.UserHasClearance(ctx.User, auth.TdbUserRoleReadOnly) {
			return NewErrorResponse(http.StatusForbidden, auth.InsufficientPermissions.Error())
		}
		if ctx.Schema != nil {
			ctx.Schema.GetLocker().RLock()
			defer ctx.Schema.GetLocker().RUnlock()
		}
	} else {
		if !ctx.Schema.UserHasClearance(ctx.User, auth.TdbUserRoleReadWrite) {
			return NewErrorResponse(http.StatusForbidden, auth.InsufficientPermissions.Error())
		}
		if ctx.Schema != nil {
			ctx.Schema.GetLocker().Lock()
			defer ctx.Schema.GetLocker().Unlock()
		}
	}

	if action.IsDBAction() {
		if !ctx.Schema.UserHasClearance(ctx.User, auth.TdbUserRoleAdmin) {
			return NewErrorResponse(http.StatusForbidden, auth.InsufficientPermissions.Error())
		}
		tdb.Locker.Lock()
		defer tdb.Locker.Unlock()
	} else if ctx.Schema == nil {
		return NewErrorResponse(http.StatusBadRequest, "no database selected")
	}

	if ctx.TxCtx == nil {
		ctx.TxCtx = transaction.NewTransactionCtx(tdb)
	}
	// TODO(Tobshub): create snapshot for use in tx

	switch action {
	case RequestActionCreateDB:
		return CreateDBReqHandler(tdb, raw)
	case RequestActionDropDB:
		return DropDBReqHandler(tdb, raw)
	case RequestActionUseDB:
		return UseDBReqHandler(tdb, raw, ctx)
	case RequestActionListDB:
		return ListDBReqHandler(tdb)
	case RequestActionDBStat:
		return DBStatReqHandler(tdb, ctx)
	case RequestActionCreateUser:
		return CreateUserReqHandler(tdb, raw)
	case RequestActionDeleteUser:
		return DeleteUserReqHandler(tdb, raw)
	case RequestActionUpdateUserRole:
		return UpdateUserRoleReqHandler(tdb, raw)
	case RequestActionCreate:
		return CreateReqHandler(ctx.Schema, raw)
	case RequestActionCreateMany:
		return CreateManyReqHandler(ctx.Schema, raw)
	case RequestActionFind:
		return FindReqHandler(ctx.Schema, raw)
	case RequestActionFindMany:
		return FindManyReqHandler(ctx.Schema, raw)
	case RequestActionDelete:
		return DeleteReqHandler(ctx.Schema, raw)
	case RequestActionDeleteMany:
		return DeleteManyReqHandler(ctx.Schema, raw)
	case RequestActionUpdate:
		return UpdateReqHandler(ctx.Schema, raw)
	case RequestActionUpdateMany:
		return UpdateManyReqHandler(ctx.Schema, raw)
	case RequestActionTransaction:
		return StartTransactionReqHandler(tdb, ctx)
	case RequestActionCommit:
		return CommitTransactionReqHandler(ctx)
	case RequestActionRollback:
		return RollbackTransactionReqHandler(ctx)
	default:
		return NewErrorResponse(http.StatusBadRequest, fmt.Sprintf("unknown action: %s", action))
	}
}
