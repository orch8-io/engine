/** Match Chrono's AutoSi timestamp representation in typed signing bytes. */
export function federationSigningBytes(unsigned: Record<string, unknown>): Buffer {
  const timestamp = (value: unknown): unknown =>
    typeof value === "string" ? value.replace(/\.000Z$/, "Z") : value;
  return Buffer.from(JSON.stringify({
    ...unsigned,
    issued_at: timestamp(unsigned.issued_at),
    expires_at: timestamp(unsigned.expires_at),
    signature: "",
  }));
}
