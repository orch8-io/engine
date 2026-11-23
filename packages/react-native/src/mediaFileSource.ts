import type { MediaUploadFile, MediaUploadTransport } from "./mediaUpload";

export const maxMediaRangeBytes = 4 * 1024 * 1024;

export interface NativeMediaFileReader {
  describeMediaFile(path: string, chunkBytes: number): Promise<Omit<MediaUploadFile, "path">>;
  readMediaFileRange(
    path: string, offset: number, length: number, identity: string | null, sha256: string | null,
  ): Promise<string>;
}

export type MediaFilePathResolver = (uploadId: string) => string | Promise<string>;

const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

function decodeRange(encoded: string, length: number): Uint8Array {
  const padding = (3 - length % 3) % 3;
  const digits = encoded.length - padding;
  if (encoded.length !== Math.ceil(length / 3) * 4 ||
      encoded.slice(digits) !== "=".repeat(padding)) {
    throw new Error("Invalid native media range");
  }
  const bytes = new Uint8Array(length);
  let bits = 0;
  let value = 0;
  let written = 0;
  for (let i = 0; i < digits; i++) {
    const digit = alphabet.indexOf(encoded[i]);
    if (digit < 0) throw new Error("Invalid native media range");
    value = (value << 6) | digit;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      bytes[written++] = value >>> bits;
    }
  }
  return bytes;
}

function validatePath(path: string): void {
  if (!path.startsWith("/") || path.includes("\0")) {
    throw new TypeError("Media upload requires an absolute local file path");
  }
}

export async function describeMediaUploadFile(
  native: NativeMediaFileReader,
  path: string,
  chunkBytes = 256 * 1024,
): Promise<MediaUploadFile> {
  validatePath(path);
  if (!Number.isSafeInteger(chunkBytes) || chunkBytes < 1 || chunkBytes > maxMediaRangeBytes) {
    throw new RangeError("Invalid media manifest chunk size");
  }
  return { ...await native.describeMediaFile(path, chunkBytes), path };
}

export function createFileMediaUploadSource(
  native: NativeMediaFileReader,
  resolvePath?: MediaFilePathResolver,
): Pick<MediaUploadTransport, "read"> {
  return {
    async read(id, offset, length, state) {
      if (!Number.isSafeInteger(offset) || offset < 0 ||
          !Number.isSafeInteger(length) || length < 1 || length > maxMediaRangeBytes ||
          !Number.isSafeInteger(offset + length)) {
        throw new RangeError("Invalid media file byte range");
      }
      const file = state?.file;
      if (file) {
        const sha256 = file.sha256[offset / file.chunkBytes];
        if (file.version !== 1 || file.length !== state.length || !file.identity ||
            length !== Math.min(file.chunkBytes, file.length - offset) ||
            !sha256 || !/^[0-9a-f]{64}$/.test(sha256)) {
          throw new Error("Invalid media file manifest for requested range");
        }
        validatePath(file.path);
        return decodeRange(
          await native.readMediaFileRange(file.path, offset, length, file.identity, sha256), length,
        );
      }
      if (!resolvePath) throw new Error("Media upload is missing its file manifest");
      const path = await resolvePath(id);
      validatePath(path);
      return decodeRange(await native.readMediaFileRange(path, offset, length, null, null), length);
    },
  };
}
