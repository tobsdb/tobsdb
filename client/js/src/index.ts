import { readFileSync } from "fs";
import path from "path";
import WebSocket from "ws";

type TobsDBOptions = {
  log: boolean;
};

// TODO: set up better logging
export default class TobsDB {
  /**
   * Connect to a TobsDB server
   *
   * @param url {string} the URL that point to the TobsDB server
   * @param db_name {string} the name of the database to run operations on in the TobsDB server
   * @param schema_path {string | undefined} the absolute path to the schema file, i.e. schema.tdb
   * */
  static async connect(
    url: string,
    db_name: string,
    options: {
      auth?: { username: string; password: string };
      schema_path?: string;
    }
  ): Promise<TobsDB> {
    const canonical_url = new URL(url);
    canonical_url.searchParams.set("db", db_name);
    options.schema_path =
      options.schema_path || path.join(process.cwd(), "schema.tdb");
    const schema_data = readFileSync(options.schema_path).toString();
    canonical_url.searchParams.set("schema", schema_data);

    const db = new TobsDB(canonical_url.toString(), options.auth, {});
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

  create(table: string, data: object) {
    return this.__query<QueryType.Unique>(QueryAction.Create, table, data);
  }

  createMany(table: string, data: object[]) {
    return this.__query<QueryType.Many>(QueryAction.CreateMany, table, data);
  }

  findUnique(table: string, where: object) {
    return this.__query<QueryType.Unique>(
      QueryAction.Find,
      table,
      undefined,
      where
    );
  }

  findMany(table: string, where: object) {
    return this.__query<QueryType.Many>(
      QueryAction.FindMany,
      table,
      undefined,
      where
    );
  }

  updateUnique(table: string, where: object, data: object) {
    return this.__query<QueryType.Unique>(
      QueryAction.Update,
      table,
      data,
      where
    );
  }

  updateMany(table: string, where: object, data: object) {
    return this.__query<QueryType.Many>(
      QueryAction.UpdateMany,
      table,
      data,
      where
    );
  }

  deleteUnique(table: string, where: object) {
    return this.__query<QueryType.Unique>(
      QueryAction.Delete,
      table,
      undefined,
      where
    );
  }

  deleteMany(table: string, where: object) {
    return this.__query<QueryType.Many>(
      QueryAction.DeleteMany,
      table,
      undefined,
      where
    );
  }
}

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
