enum Errors {
  DisconnClient = "ErrDisconnClient",
  CannotConnectClient = "ErrCannotConnClient",
  ClosedClient = "ErrClosedClient",
}

class TdbError extends Error {
  constructor(
    public readonly name: string,
    message: string,
  ) {
    super(message);
  }
}

export class DisconnectedError extends TdbError {
  constructor() {
    super(Errors.DisconnClient, "Not connected to a TobsDB server");
  }
}

export class CannotConnectError extends TdbError {
  constructor(
    public readonly reason: string,
    public readonly stack?: string,
  ) {
    super(Errors.CannotConnectClient, "Cannot connect to TobsDB server");
  }
}

export class ClosedError extends TdbError {
  constructor(
    public readonly code: number,
    public readonly reason: string,
  ) {
    super(Errors.ClosedClient, "TobsDB server closed connection");
  }
}
