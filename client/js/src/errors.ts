enum Errors {
  DisconnClient = "ErrDisconnClient",
  CannotConnectClient = "ErrCannotConnClient",
  ClosedClient = "ErrClosedClient",
}

export class DisconnectedError extends Error {
  constructor() {
    super("Not connected to a TobsDB server");
    this.name = Errors.DisconnClient;
  }
}

export class CannotConnectError extends Error {
  constructor(public readonly reason: string, stack?: string) {
    super("Cannot connect to TobsDB server");
    this.name = Errors.CannotConnectClient;
    this.stack = stack;
  }
}

export class ClosedError extends Error {
  constructor(public readonly code: number, public readonly reason: string) {
    super("TobsDB server closed connection");
    this.name = Errors.ClosedClient;
  }
}
