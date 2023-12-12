import { existsSync, readFileSync } from "fs";
import path from "path";
import WebSocket from "ws";
import { logger } from "./logger";
import { CannotConnectError, ClosedError, DisconnectedError } from "./errors";
import { GenClientId } from "./client-id";

type TobsDBOptions = {
  username: string;
  password: string;
  log?: boolean;
  debug?: boolean;
  schema_path: string | null;
};

function defaultSchemaPath(schema_path: string | null = "./schema.tdb") {
  if (schema_path === null) {
    return null;
  }
  return schema_path;
}

export default class TobsDB<const Schema extends Record<string, object>> {
  // TODO: replace with shell call to tdb-validate
  static async validateSchema(
    url: string,
    schema_path?: string,
  ): Promise<string> {
    const canonical_url = new URL(url);
    const schema_data = readFileSync(
      schema_path || path.join(process.cwd(), "schema.tdb"),
    ).toString();
    canonical_url.searchParams.set("schema", schema_data);
    canonical_url.searchParams.set("check_schema", "true");

    const ws = new WebSocket(canonical_url);

    return new Promise<string>((resolve, reject) => {
      ws.on("close", (code, message) => {
        if (code === 1000) {
          return resolve(message.toString());
        }
        reject(new ClosedError(code, message.toString()));
      });
      ws.on("error", (e) => reject(new CannotConnectError(e.message, e.stack)));
    });
  }

  public readonly url: URL;
  public schema: { from_file?: string; arg?: string };
  private readonly options: TobsDBOptions;
  private ws?: WebSocket;
  private logger: ReturnType<typeof logger>;
  private handlers: Map<number, (data: any) => void>;

  /**
   * @param url {string} TobsDB server url
   * @param db_name {string} TobsDB database name
   * @param options {Partial<TobsDBOptions>}
   */
  constructor(
    url: string,
    db_name: string,
    options: Partial<TobsDBOptions> = {},
  ) {
    this.logger = logger(options);
    this.handlers = new Map();
    const canonical_url = new URL(url);
    canonical_url.searchParams.set("db", db_name);
    this.url = canonical_url;
    this.options = {
      ...options,
      username: options.username ?? "",
      password: options.password ?? "",
      schema_path: defaultSchemaPath(options.schema_path),
    };
    this.schema = {};
  }

  private formatSchema() {
    let data = "";
    if (this.schema.from_file) {
      data += this.schema.from_file;
    }
    if (this.schema.arg) {
      data += "\n";
      data += this.schema.arg;
    }
    return data;
  }

  private formatAuthorizationHeader() {
    return `${this.options.username}:${this.options.password}`;
  }

  /** Connect to a TobsDB Server.
   * If this instance of the client is already connected, does not attempt to connect again.
   *
   * The schema is read from the path provided to the {options.schema_path} in the constructor.
   * If no path is provided, it checks the current working directory for a `schema.tdb` file and (if it exists) uses that.
   *
   * Optionally, you can provide a string to {schema}. If a schema was read from file, {schema} will be appended to it
   * If there was not, {schema} will be used as the schema.
   *
   * `connect` only performs the read on the first call, so it will not update if the schema file changes during runtime.
   *
   * @param schema {string | undefined} optional schema string
   * */
  connect(schema?: string) {
    if (this.ws && this.ws.readyState < WebSocket.CLOSING) return;

    if (!this.schema.from_file) {
      if (this.options.schema_path && existsSync(this.options.schema_path)) {
        this.schema.from_file = readFileSync(this.options.schema_path, {
          encoding: "utf8",
        });
      }
    }

    if (schema) {
      this.schema.arg = schema;
    }

    this.url.searchParams.set("schema", this.formatSchema());

    this.ws = new WebSocket(this.url, {
      headers: { Authorization: this.formatAuthorizationHeader() },
    });

    // TODO: this wrongly handles immediate closing connections
    return new Promise<void>((resolve, reject) => {
      if (!this.ws) return reject(new CannotConnectError("No WebSocket"));
      if (this.ws.readyState >= WebSocket.OPEN) return resolve();

      this.ws.once("open", () => {
        this.logger.info("Connected to TobsDB server");
        resolve();
      });

      this.ws.once("error", (err) => {
        this.logger.error("Error connecting to TobsDB server", err);
        reject(new CannotConnectError(err.message, err.stack));
      });

      this.ws.on("message", (data) => {
        const msg = JSON.parse(data.toString()) as TDBResponse<
          QueryType.Unique | QueryType.Many,
          object
        >;
        const handler = this.handlers.get(msg.__tdb_client_req_id__);
        if (handler) {
          this.logger.debug("calling handler", msg.__tdb_client_req_id__);
          handler(msg);
          this.handlers.delete(msg.__tdb_client_req_id__);
          return;
        }

        this.logger.warn(
          "received message without handler",
          msg.__tdb_client_req_id__,
        );
      });
    });
  }

  /** Gracefully disconnect */
  async disconnect() {
    if (!this.ws || this.ws.readyState >= WebSocket.CLOSING) return;
    this.ws.close(1000);
    this.logger.info("Disconnected from TobsDB server");
  }

  private async __query<
    T extends QueryType.Unique | QueryType.Many,
    const Table extends keyof Schema & string,
  >(
    action: QueryAction,
    table: Table,
    data: object | object[] | undefined,
    where?: object | undefined,
  ) {
    await this.connect();
    if (!this.ws || this.ws.readyState >= WebSocket.CLOSING) {
      throw new DisconnectedError();
    }
    const __tdb_client_req_id__ = GenClientId();
    const q = JSON.stringify({
      action,
      table,
      data,
      where,
      __tdb_client_req_id__,
    });
    this.logger.info(action, table);
    this.ws.send(q);
    const res = await new Promise<
      TDBResponse<T, TDBResponseData<Schema[Table]>>
    >((resolve, _reject) => {
      // TODO: when to reject???
      const handler = (data: any) => resolve(data);
      this.handlers.set(__tdb_client_req_id__, handler);
    });
    this.logger.debug(action, table, "(DONE)");
    return res;
  }

  create<const Table extends keyof Schema & string>(
    table: Table,
    data: CreateData<Schema[Table]>,
  ) {
    return this.__query<QueryType.Unique, Table>(
      QueryAction.Create,
      table,
      data,
    );
  }

  createMany<const Table extends keyof Schema & string>(
    table: Table,
    data: CreateData<Schema[Table]>[],
  ) {
    return this.__query<QueryType.Many, Table>(
      QueryAction.CreateMany,
      table,
      data,
    );
  }

  findUnique<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhereUnique<Schema[Table]>,
  ) {
    return this.__query<QueryType.Unique, Table>(
      QueryAction.Find,
      table,
      undefined,
      where,
    );
  }

  findMany<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhereMany<Schema[Table]>,
  ) {
    return this.__query<QueryType.Many, Table>(
      QueryAction.FindMany,
      table,
      undefined,
      where,
    );
  }

  updateUnique<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhereUnique<Schema[Table]>,
    data: UpdateData<Schema[Table]>,
  ) {
    return this.__query<QueryType.Unique, Table>(
      QueryAction.Update,
      table,
      data,
      where,
    );
  }

  updateMany<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhereMany<Schema[Table]>,
    data: UpdateData<Schema[Table]>,
  ) {
    return this.__query<QueryType.Many, Table>(
      QueryAction.UpdateMany,
      table,
      data,
      where,
    );
  }

  deleteUnique<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhereUnique<Schema[Table]>,
  ) {
    return this.__query<QueryType.Unique, Table>(
      QueryAction.Delete,
      table,
      undefined,
      where,
    );
  }

  deleteMany<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhereMany<Schema[Table]>,
  ) {
    return this.__query<QueryType.Many, Table>(
      QueryAction.DeleteMany,
      table,
      undefined,
      where,
    );
  }

  // DON'T TOUCH THIS!
  __allDone() {
    return this.handlers.size > 0 ? false : true;
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

export type CreateData<Table extends object> = ParseFieldProps<
  OptDefaultFields<Table>
>;

type ParseFieldProps<Table> = {
  [K in keyof Table]: ParseFieldProp<Table[K]>;
};

export type ParseFieldProp<T> = NonNullable<T> extends FieldProp<any, any>
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
//   t.findMany("hello", { id: { eq: 69 }, world: "deez" });
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
