import net from "net";

export class TcpClient {
  constructor(host, port) {
    this.host = host;
    this.port = port;
    this.connected = false;

    const client = new net.Socket();
    client.setNoDelay(true);

    this.conn = client;
  }

  async connect() {
    if (this.connected) return;

    await new Promise((res, rej) => {
      this.conn.connect(this.port, this.host, () => res());
      this.conn.on("error", (e) => rej(e));
      this.conn.on("close", (e) => console.log("CLOSED", e));
    });
    this.connected = true;
  }

  async send(message) {
    await this.connect();
    await new Promise((res) => {
      const isFlushed = this.conn.write(Buffer.from(message));
      if (!isFlushed) {
        this.conn.once("drain", () => res());
      } else {
        process.nextTick(() => res());
      }
    });

    // process first chunk
    let [size, raw] = await new Promise((res) => {
      this.conn.once("data", (chunk) => {
        const size = chunk.readUInt32BE(0);
        const raw = chunk.toString().substring(4);
        res([size, raw]);
      });
    });

    // loop until all data is read
    while (size > raw.length) {
      const chunk = await new Promise((res) => {
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
}
