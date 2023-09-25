import WebSocket from "ws";

export default class TobsDB {
  private static ws: WebSocket;

  static connect(url: string): Promise<TobsDB> {
    this.ws = new WebSocket(url);
    return new Promise<TobsDB>((resolve, reject) => {
      this.ws.on("open", () => {
        resolve(new TobsDB(url));
      });
      this.ws.on("error", (err) => {
        reject(err);
      });
    });
  }

  constructor(public readonly url: string) {}

  async disconnect() {
    TobsDB.ws.close(1000);
  }

  create(table: string, data: object) {
    TobsDB.ws.send(JSON.stringify({ action: "create", table, data }));
    return new Promise((res) => {
      TobsDB.ws.once("message", (ev) => {
        const data = Buffer.from(ev.toString()).toString();
        res(JSON.parse(data));
      });
    });
  }
}
