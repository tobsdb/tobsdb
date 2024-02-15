enum Errors {
  DisconnClient = "ErrDisconnClient",
  CannotConnectClient = "ErrCannotConnClient",
  ClosedClient = "ErrClosedClient",
}

class TdbError extends Error {
  constructor(
    public readonly name: Errors,
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
    public readonly error: unknown,
  ) {
    super(Errors.CannotConnectClient, "Cannot connect to TobsDB server");
  }
}

export class ClosedError extends TdbError {
  constructor() {
    super(Errors.ClosedClient, "TobsDB server closed connection");
  }
}
