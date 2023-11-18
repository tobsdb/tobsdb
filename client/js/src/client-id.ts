import crypto from "node:crypto";

export function GenClientId() {
  return crypto.randomUUID();
}
