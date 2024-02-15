import net from "net";

export class TcpClient {
  private conn: net.Socket;
  public connected: boolean = false;
  constructor(
    public readonly host: string,
    public readonly port: number,
  ) {
    const client = new net.Socket();
    client.setNoDelay(true);

    this.conn = client;
  }

  async connect(): Promise<void> {
    if (this.connected) return;

    await new Promise<void>((res, rej) => {
      this.conn.connect(this.port, this.host, () => res());
      this.conn.on("error", (e) => rej(e));
    });
    this.connected = true;
  }

  async send(message: string): Promise<string> {
    await this.connect();

    await new Promise<void>((res) => {
      const isFlushed = this.conn.write(Buffer.from(message));
      if (!isFlushed) {
        this.conn.once("drain", () => res());
      } else {
        process.nextTick(() => res());
      }
    });

    // process first chunk
    let [size, raw] = await new Promise<[number, string]>((res) => {
      this.conn.once("data", (chunk) => {
        const size = chunk.readUInt32BE(0);
        const raw = chunk.toString().substring(4);
        res([size, raw]);
      });
    });

    // loop until all data is read
    while (size > raw.length) {
      const chunk = await new Promise<Buffer>((res) => {
        this.conn.once("data", (chunk) => res(chunk));
      });
      raw += chunk.toString();
    }

    return raw;
  }

  close() {
    this.connected = false;
    this.conn.end();
    this.conn.destroy();
  }

  __allDone(): boolean {
    return this.conn.listenerCount("data") === 0;
  }
}
