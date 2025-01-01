import { existsSync, readFileSync } from "fs";
import { logger } from "./logger";
import { CannotConnectError, ClosedError, DisconnectedError } from "./errors";
import { ClientId, GenClientId } from "./client-id";
import { TcpClient } from "./tcp";

type TobsDBOptions = {
  log?: boolean;
  debug?: boolean;
};

type TobsdbConnectionInfo = {
  /** TobsDB server url host */
  host: string;
  /** TobsDB server url port */
  port: number;
  /** Tobsdb database name */
  db: string;
  /** Tobsdb username */
  username?: string;
  /** Tobsdb user password */
  password?: string;
  /** Path to schema.tdb
   *  If `null`, no attempt is made to read a schema.tdb file
   *  If `undefined`, the current working directory is checked for a `schema.tdb` file
   * */
  schemaPath?: string | null;
};

function defaultSchemaPath(schemaPath?: string | null) {
  return schemaPath === undefined ? "./schema.tdb" : schemaPath;
}

/**
 * Usage:
 *
 * const db = new TobsDB("localhost", 7085, "example");
 * await db.connect(process.env.TDB_USER, process.env.TDB_PASS);
 *
 * */
export default class TobsDB<const Schema extends Record<string, object>> {
  public readonly schema?: string;
  private client: TcpClient;
  private logger: ReturnType<typeof logger>;
  private connected: boolean = false;

  /**
   * @param connectionInfo {TobsdbConnectionInfo} TobsDB connection params
   * @param options {Partial<TobsDBOptions>}
   */
  constructor(
    private readonly connectionInfo: TobsdbConnectionInfo,
    options: Partial<TobsDBOptions> = {},
  ) {
    this.logger = logger(options);
    const schemaPath = defaultSchemaPath(connectionInfo.schemaPath);
    if (schemaPath && existsSync(schemaPath)) {
      this.schema = readFileSync(schemaPath, {
        encoding: "utf8",
      });
    }
    this.client = new TcpClient(connectionInfo.host, connectionInfo.port);
  }

  /** Connect to a TobsDB Server.
   * If this instance of the client is already connected, no further attempt to connect is made.
   *
   * The schema is read from the path provided to the {connectionInfo.schemaPath} in the constructor.
   * If no path is provided, it checks the current working directory for a `schema.tdb` file and (if it exists) uses that.
   * */
  async connect() {
    if (this.connected) return;

    const connectionRequest = {
      schema: this.schema,
      db: this.connectionInfo.db,
      username: this.connectionInfo.username,
      password: this.connectionInfo.password,
      tryConnect: true,
    };

    try {
      await this.client.connect();
      await this.client.send(JSON.stringify(connectionRequest));
      this.connected = true;
    } catch (e) {
      throw new CannotConnectError(e);
    }
  }

  /** Gracefully disconnect */
  async disconnect() {
    this.client.close();
    this.logger.info("Disconnected from TobsDB server");
  }

  private async __query<
    T extends QueryType.Unique | QueryType.Many,
    const Table extends keyof Schema & string,
  >(props: {
    action: QueryAction;
    table: Table;
    data?: object | object[];
    where?: object;
    take?: number;
    cursor?: object;
    orderBy?: object;
  }): Promise<TDBResponse<T, TDBResponseData<Schema[Table]>>> {
    await this.connect();
    if (!this.client.connected) {
      throw new DisconnectedError();
    }

    const q = JSON.stringify({ ...props, [ClientId]: GenClientId() });
    this.logger.info(props.action, props.table);
    const raw = await this.client.send(q);
    this.logger.debug("(DONE)", props.action, props.table);
    return JSON.parse(raw);
  }

  create<const Table extends keyof Schema & string>(
    table: Table,
    data: CreateData<Schema[Table]>,
  ) {
    return this.__query<QueryType.Unique, Table>({
      action: QueryAction.Create,
      table,
      data,
    });
  }

  createMany<const Table extends keyof Schema & string>(
    table: Table,
    data: CreateData<Schema[Table]>[],
  ) {
    return this.__query<QueryType.Many, Table>({
      action: QueryAction.CreateMany,
      table,
      data,
    });
  }

  findUnique<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhereUnique<Schema[Table]>,
  ) {
    return this.__query<QueryType.Unique, Table>({
      action: QueryAction.Find,
      table,
      where,
    });
  }

  findMany<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhereMany<Schema[Table]>,
    control?: SelectControl<Schema[Table]>,
  ) {
    return this.__query<QueryType.Many, Table>({
      action: QueryAction.FindMany,
      table,
      where,
      ...control,
    });
  }

  updateUnique<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhereUnique<Schema[Table]>,
    data: UpdateData<Schema[Table]>,
  ) {
    return this.__query<QueryType.Unique, Table>({
      action: QueryAction.Update,
      table,
      data,
      where,
    });
  }

  updateMany<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhereMany<Schema[Table]>,
    data: UpdateData<Schema[Table]>,
  ) {
    return this.__query<QueryType.Many, Table>({
      action: QueryAction.UpdateMany,
      table,
      data,
      where,
    });
  }

  deleteUnique<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhereUnique<Schema[Table]>,
  ) {
    return this.__query<QueryType.Unique, Table>({
      action: QueryAction.Delete,
      table,
      where,
    });
  }

  deleteMany<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhereMany<Schema[Table]>,
  ) {
    return this.__query<QueryType.Many, Table>({
      action: QueryAction.DeleteMany,
      table,
      where,
    });
  }

  __allDone() {
    return this.client.__allDone();
  }
}

interface FieldProp<T, N> {
  type: T;
  prop: N;
}

/** field has `key(primary)` in the schema */
export interface PrimaryKey<T> extends FieldProp<T, "primaryKey"> {}
/** field has `unique(true)` in the schema  */
export interface Unique<T> extends FieldProp<T, "unique"> {}
/** field has `default` in the schema */
export interface Default<T> extends FieldProp<T, "default"> {}

// TODO: remove PrimaryKey keys from this
export type CreateData<Table extends object> = ParseFieldProps<
  OptDefaultFields<Table>
>;

type ParseFieldProps<Table> = {
  [K in keyof Table]: ParseFieldProp<Table[K]>;
};

export type ParseFieldProp<T> =
  NonNullable<T> extends FieldProp<any, any>
    ? NonNullable<T>["type"]
    : NonNullable<T>;

// courtesy of Maya <3
type OptDefaultFields<Table> = {
  [K in keyof Table as NonNullable<Table[K]> extends
    | PrimaryKey<any>
    | Default<any>
    ? K
    : never]?: NonNullable<Table[K]> extends PrimaryKey<any> | Default<any>
    ? ParseFieldProp<NonNullable<Table[K]>["type"]>
    : never;
} & {
  [k in keyof Table as NonNullable<Table[k]> extends
    | PrimaryKey<any>
    | Default<any>
    ? never
    : k]: Table[k];
};

type RequireAtLeastOne<T> = Pick<T, Exclude<keyof T, keyof T>> &
  {
    [K in keyof T]-?: Required<Pick<T, K>> &
      Partial<Pick<T, Exclude<keyof T, K>>>;
  }[keyof T];

type QueryWhereUnique<Table extends object> = RequireAtLeastOne<{
  [K in keyof Table as NonNullable<Table[K]> extends
    | PrimaryKey<any>
    | Unique<any>
    ? K
    : never]: NonNullable<Table[K]> extends PrimaryKey<any>
    ? ParseFieldProp<NonNullable<Table[K]>["type"]>
    : NonNullable<Table[K]> extends Unique<any>
      ? undefined extends Table[K]
        ? ParseFieldProp<NonNullable<Table[K]>["type"]> | null
        : ParseFieldProp<NonNullable<Table[K]>["type"]>
      : never;
}>;

type QueryWhereMany<Table extends object> = Partial<{
  [K in keyof Table]:
    | DynamicWhere<ParseFieldProp<Table[K]>>
    | (undefined extends Table[K]
        ? ParseFieldProp<Table[K]> | null
        : ParseFieldProp<Table[K]>);
}>;

// support dynamic queries
type DynamicWhere<T> = T extends number
  ? RequireAtLeastOne<{
      gt?: number;
      lt?: number;
      lte?: number;
      gte?: number;
      eq?: number;
      neq?: number;
    }>
  : T extends string
    ? RequireAtLeastOne<{
        contains?: string;
        startsWith?: string;
        endsWith?: string;
      }>
    : T;

type SelectControl<Table extends object> = {
  take?: number;
  cursor?: RequireAtLeastOne<QueryWhereMany<Table>>;
  orderBy?: RequireAtLeastOne<{
    [K in keyof Table as ParseFieldProp<Table[K]> extends any[] ? never : K]?:
      | "asc"
      | "desc";
  }>;
};

type UpdateData<Table extends object> = {
  [K in keyof Table]?:
    | DynamicUpdate<ParseFieldProp<Table[K]>>
    | (undefined extends Table[K]
        ? ParseFieldProp<Table[K]> | null
        : ParseFieldProp<Table[K]>);
};

// support dynamic updates
type DynamicUpdate<T> = T extends number
  ? RequireAtLeastOne<{
      increment?: number;
      decrement?: number;
    }>
  : T extends Array<any>
    ? RequireAtLeastOne<{ push?: T }>
    : never;

enum QueryAction {
  Create = "create",
  CreateMany = "createMany",
  Update = "updateUnique",
  UpdateMany = "updateMany",
  Delete = "deleteUnique",
  DeleteMany = "deleteMany",
  Find = "findUnique",
  FindMany = "findMany",
}

enum QueryType {
  Unique,
  Many,
  Schema,
}

export interface TDBResponse<U extends QueryType, Table extends object = {}> {
  status: number;
  message: string;
  data: U extends QueryType.Unique
    ? Table
    : U extends QueryType.Many
      ? Table[]
      : string;
  __tdb_client_req_id__: number;
}

export type TDBResponseData<Table> = {
  [K in keyof Table]-?: undefined extends Table[K]
    ? ParseFieldProp<Table[K]> | null
    : ParseFieldProp<Table[K]>;
};

// async () => {
//   type DB = {
//     hello: {
//       id: PrimaryKey<number>;
//       world: string;
//       hi: Unique<string>;
//       deez?: string;
//     };
//     world: {
//       id: PrimaryKey<number>;
//       pew?: Unique<string>;
//       hello: string;
//       vec: number[];
//     };
//   };

//   const t = new TobsDB<DB>("", "", {});
//   t.connect();

//   const p = await t.create("hello", { world: "string", hi: "string" });
//   p.data.deez;
//   t.findUnique("hello", { id: 0 });
//   t.findUnique("world", { id: 0, pew: "pew" });
//   t.findMany(
//     "hello",
//     { id: { eq: 69 }, world: "deez" },
//     { orderBy: { id: "desc" } },
//   );
//   t.findMany(
//     "world",
//     { id: { eq: 69 }, hello: "deez" },
//     {
//       orderBy: { pew: "desc" },
//       take: 5,
//       cursor: { id: 10 },
//     },
//   );
//   t.updateUnique(
//     "hello",
//     { hi: "string" },
//     { id: { increment: 1 }, world: "string" },
//   );
//   t.updateMany(
//     "hello",
//     { id: { lte: 69 }, hi: { contains: "deez" } },
//     { id: { decrement: 1 }, deez: null },
//   );
//   t.updateUnique("world", { id: 0 }, { pew: null });
//   t.updateMany("world", { id: 0 }, { vec: { push: [69] } });
//   t.deleteUnique("hello", { id: 0 });
//   t.deleteMany("hello", { id: { lte: 69 } });
// };
