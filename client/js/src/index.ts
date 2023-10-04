import { readFileSync } from "fs";
import path from "path";
import WebSocket from "ws";

type TobsDBOptions = {
  log: boolean;
};

// TODO: set up better logging
export default class TobsDB<const Schema extends Record<string, object>> {
  /** Connect to a TobsDB server */
  static async connect<const SchemaType extends Record<string, object>>(
    url: string,
    db_name: string,
    options: {
      auth?: { username: string; password: string };
      schema_path?: string;
    }
  ): Promise<TobsDB<SchemaType>> {
    const canonical_url = new URL(url);
    canonical_url.searchParams.set("db", db_name);
    options.schema_path =
      options.schema_path || path.join(process.cwd(), "schema.tdb");
    const schema_data = readFileSync(options.schema_path).toString();
    canonical_url.searchParams.set("schema", schema_data);

    const db = new TobsDB<SchemaType>(
      canonical_url.toString(),
      options.auth,
      {}
    );
    await new Promise<void>((res, rej) => {
      db.ws.once("open", res);
      db.ws.once("error", rej);
    });
    return db;
  }

  static async validateSchema(
    url: string,
    schema_path?: string
  ): Promise<TDBSchemaValidationResponse> {
    const canonical_url = new URL(url);
    const schema_data = readFileSync(
      schema_path || path.join(process.cwd(), "schema.tdb")
    ).toString();
    canonical_url.searchParams.set("schema", schema_data);
    canonical_url.searchParams.set("check_schema", "true");

    const res: TDBResponse<QueryType.Schema> = await fetch(canonical_url).then(
      (res) => res.json()
    );

    if (res.status === 200) {
      return { ok: true, message: res.message };
    }
    return { ok: false, message: res.message };
  }

  private ws: WebSocket;

  constructor(
    public readonly url: string,
    auth: { username: string; password: string } = {
      username: "",
      password: "",
    },
    public readonly options: Partial<TobsDBOptions>
  ) {
    this.ws = new WebSocket(url, {
      headers: { Authorization: `${auth.username}:${auth.password}` },
    });
  }

  async disconnect() {
    this.ws.close(1000);
  }

  private __query<T extends QueryType>(
    action: QueryAction,
    table: string,
    data: object | object[] | undefined,
    where?: object | undefined
  ) {
    const q = JSON.stringify({ action, table, data, where });
    if (this.options.log) {
      console.log("Query:", q);
    }
    this.ws.send(q);
    return new Promise<TDBResponse<T>>((res) => {
      this.ws.once("message", (ev) => {
        const data = Buffer.from(ev.toString()).toString();
        res(JSON.parse(data));
      });
    });
  }

  create<const Table extends keyof Schema & string>(
    table: Table,
    data: CreateData<Schema[Table]>
  ) {
    return this.__query<QueryType.Unique>(QueryAction.Create, table, data);
  }

  createMany<const Table extends keyof Schema & string>(
    table: Table,
    data: CreateData<Schema[Table]>[]
  ) {
    return this.__query<QueryType.Many>(QueryAction.CreateMany, table, data);
  }

  findUnique<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhere<Schema[Table], QueryType.Unique>
  ) {
    return this.__query<QueryType.Unique>(
      QueryAction.Find,
      table,
      undefined,
      where
    );
  }

  findMany<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhere<Schema[Table], QueryType.Many>
  ) {
    return this.__query<QueryType.Many>(
      QueryAction.FindMany,
      table,
      undefined,
      where
    );
  }

  updateUnique<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhere<Schema[Table], QueryType.Unique>,
    data: UpdateData<Schema[Table]>
  ) {
    return this.__query<QueryType.Unique>(
      QueryAction.Update,
      table,
      data,
      where
    );
  }

  updateMany<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhere<Schema[Table], QueryType.Many>,
    data: UpdateData<Schema[Table]>
  ) {
    return this.__query<QueryType.Many>(
      QueryAction.UpdateMany,
      table,
      data,
      where
    );
  }

  deleteUnique<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhere<Schema[Table], QueryType.Unique>
  ) {
    return this.__query<QueryType.Unique>(
      QueryAction.Delete,
      table,
      undefined,
      where
    );
  }

  deleteMany<const Table extends keyof Schema & string>(
    table: Table,
    where: QueryWhere<Schema[Table], QueryType.Many>
  ) {
    return this.__query<QueryType.Many>(
      QueryAction.DeleteMany,
      table,
      undefined,
      where
    );
  }
}

/** TobsDB primary key */
interface FieldProp<T, N> {
  type: T;
  prop: N;
}

/** Make field the primary key of the table */
export interface PrimaryKey<T> extends FieldProp<T, "primaryKey"> {}
/** Treat field as unique in the table  */
export interface Unique<T> extends FieldProp<T, "unique"> {}

export type CreateData<Table extends object> = ParseFieldProps<
  OptPrimaryKey<Table>
>;

type ParseFieldProps<Table> = {
  [K in keyof Table]: Table[K] extends FieldProp<any, any>
    ? Table[K]["type"]
    : Table[K];
};

// courtesy of Maya <3
type OptPrimaryKey<Table> = {
  [K in keyof Table as Table[K] extends PrimaryKey<any>
    ? K
    : never]?: Table[K] extends PrimaryKey<any> ? Table[K]["type"] : never;
} & {
  [k in keyof Table as Table[k] extends PrimaryKey<any> ? never : k]: Table[k];
};

type RequireAtLeastOne<T> = Pick<T, Exclude<keyof T, keyof T>> &
  {
    [K in keyof T]-?: Required<Pick<T, K>> &
      Partial<Pick<T, Exclude<keyof T, K>>>;
  }[keyof T];

type QueryWhere<
  Table extends object,
  Type extends QueryType
> = Type extends QueryType.Unique
  ? RequireAtLeastOne<QueryWhereUnique<Table>>
  : QueryWhereMany<Table>;

type QueryWhereUnique<Table extends object> = {
  [K in keyof Table as Table[K] extends PrimaryKey<any>
    ? K
    : Table[K] extends Unique<any>
    ? K
    : never]:
    | (Table[K] extends PrimaryKey<any> ? Table[K]["type"] : never)
    | (Table[K] extends Unique<any> ? Table[K]["type"] : never);
};

type QueryWhereMany<Table extends object> = Partial<{
  [K in keyof Table]:
    | (Table[K] extends PrimaryKey<any>
        ? DynamicWhere<Table[K]["type"]>
        : never)
    | DynamicWhere<Table[K]>
    | (Table[K] extends PrimaryKey<any> ? Table[K]["type"] : Table[K]);
}>;

// support dynamic queries
type DynamicWhere<T> = T extends number
  ? {
      gt?: number;
      lt?: number;
      lte?: number;
      gte?: number;
      eq?: number;
      neq?: number;
    }
  : T extends string
  ? { contains?: string; startsWith?: string; endsWith?: string }
  : never;

type UpdateData<Table extends object> = _UpdateData<OptPrimaryKey<Table>>;

type _UpdateData<Table> = {
  [K in keyof Table]?: DynamicUpdate<Table[K]> | Table[K];
};

// support dynamic updates
type DynamicUpdate<T> = T extends number
  ? {
      increment?: number;
      decrement?: number;
    }
  : T extends Array<any>
  ? { push?: T }
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

export type QueryActionCreate = QueryAction.Create;
export type QueryActionCreateMany = QueryAction.CreateMany;
export type QueryActionUpdate = QueryAction.Update;
export type QueryActionUpdateMany = QueryAction.UpdateMany;
export type QueryActionDelete = QueryAction.Delete;
export type QueryActionDeleteMany = QueryAction.DeleteMany;
export type QueryActionFind = QueryAction.Find;
export type QueryActionFindMany = QueryAction.FindMany;

enum QueryType {
  Unique,
  Many,
  Schema,
}

export type QueryTypeUnique = QueryType.Unique;
export type QueryTypeMany = QueryType.Many;

export interface TDBResponse<U extends QueryType> {
  status: number;
  message: string;
  data: U extends QueryType.Unique
    ? object
    : U extends QueryType.Many
    ? object[]
    : string;
}

export interface TDBSchemaValidationResponse {
  ok: boolean;
  message: string;
}

// async () => {
//   type DB = {
//     hello: {
//       id: PrimaryKey<number>;
//       world: string;
//       hi: Unique<string>;
//       deez?: string;
//     };
//   };

//   const t = await TobsDB.connect<DB>("", "", {});

//   t.create("hello", { world: "", hi: "string" });
//   t.findUnique("hello", { id: 0 });
//   t.findMany("hello", { id: { eq: 69 }, world: "deez" });
//   t.updateUnique()
//   t.updateMany("hello", { id: { lte: 69 } }, { id: { decrement: 1 } });
//   t.deleteUnique("hello", {id: 0});
//   t.deleteMany("hello", { id: { lte: 69 } });
// };
