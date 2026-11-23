import type { MediaUploadFile, MediaUploadTransport } from "./mediaUpload";

export type MediaUploadRequest = (path: string, init: RequestInit) => Promise<Response>;

/** Use an account-bound request function that resolves paths against the API's /v1 prefix. */
export function createHttpMediaUploadTransport(
  instanceId: string,
  request: MediaUploadRequest,
  source: Pick<MediaUploadTransport, "read">,
): MediaUploadTransport & { begin(id: string, file: MediaUploadFile): Promise<void> } {
  const path = (id: string) => `/instances/${encodeURIComponent(instanceId)}/media-uploads/${encodeURIComponent(id)}`;
  return {
    read: source.read.bind(source),
    async begin(id, file) {
      if (!file.fileSha256 || !/^[0-9a-f]{64}$/.test(file.fileSha256)) {
        throw new Error("Media upload requires a whole-file SHA-256 digest");
      }
      const response = await request(path(id), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ length: file.length, chunk_bytes: file.chunkBytes, sha256: file.fileSha256 }),
      });
      if (!response.ok) throw new Error(`Media upload initialization failed (${response.status})`);
    },
    async append(id, offset, bytes) {
      const response = await request(`${path(id)}/ranges/${offset}`, {
        method: "PUT",
        headers: { "Content-Type": "application/octet-stream" },
        body: new Uint8Array(bytes).buffer,
      });
      if (!response.ok) throw new Error(`Media range upload failed (${response.status})`);
      const reply: unknown = await response.json();
      const end = (reply as { offset?: unknown } | null)?.offset;
      if (typeof end !== "number" || !Number.isSafeInteger(end) || end !== offset + bytes.byteLength) {
        throw new Error("Unexpected media acknowledgement offset");
      }
      return end;
    },
  };
}
