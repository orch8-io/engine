export interface MediaUploadState {
  id: string;
  length: number;
  offset: number;
  status: "pending" | "uploaded";
}

/** Persist the upload record next to an immutable local blob before scheduling it. */
export interface MediaUploadStore {
  load(id: string): Promise<MediaUploadState | null>;
  compareAndSet(id: string, before: MediaUploadState, after: MediaUploadState): Promise<boolean>;
}

export interface MediaUploadTransport {
  read(id: string, offset: number, length: number): Promise<Uint8Array>;
  /**
   * Persist the exact range once for this upload ID and return its end offset.
   * A duplicate request must return the same offset without duplicating data.
   * For the final range, acknowledge only after the whole artifact is durable.
   */
  append(id: string, offset: number, bytes: Uint8Array): Promise<number>;
}

export interface MediaUploadOptions {
  network: "offline" | "metered" | "unmetered";
  allowMetered?: boolean;
  chunkBytes?: number;
  maxBytes?: number;
  maxChunks?: number;
}

export type MediaUploadProgress = { status: "deferred" | "pending" | "uploaded"; offset: number };

const defaultMaxBytes = 128 * 1024 * 1024;
const defaultChunkBytes = 256 * 1024;

export class DurableMediaUploader {
  private readonly store: MediaUploadStore;
  private readonly transport: MediaUploadTransport;

  constructor(store: MediaUploadStore, transport: MediaUploadTransport) {
    this.store = store;
    this.transport = transport;
  }

  async advance(id: string, options: MediaUploadOptions): Promise<MediaUploadProgress> {
    const maxBytes = options.maxBytes ?? defaultMaxBytes;
    const chunkBytes = options.chunkBytes ?? defaultChunkBytes;
    const maxChunks = options.maxChunks ?? 1;
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 ||
        !Number.isSafeInteger(chunkBytes) || chunkBytes < 1 || chunkBytes > 4 * 1024 * 1024 ||
        !Number.isSafeInteger(maxChunks) || maxChunks < 1 || maxChunks > 64) {
      throw new RangeError("Invalid media upload limits");
    }
    for (let chunk = 0; chunk < maxChunks; chunk++) {
      const state = await this.load(id, maxBytes);
      if (state.status === "uploaded") return { status: state.status, offset: state.offset };
      if (options.network === "offline" || (options.network === "metered" && !options.allowMetered)) {
        return { status: "deferred", offset: state.offset };
      }
      const length = Math.min(chunkBytes, state.length - state.offset);
      const bytes = await this.transport.read(id, state.offset, length);
      if (bytes.byteLength !== length) throw new Error("Media source length changed");
      const offset = await this.transport.append(id, state.offset, bytes);
      if (offset !== state.offset + length) throw new Error("Unexpected media acknowledgement offset");
      const next = { ...state, offset, status: offset === state.length ? "uploaded" : "pending" } as const;
      await this.store.compareAndSet(id, state, next);
    }
    const state = await this.load(id, maxBytes);
    return { status: state.status, offset: state.offset };
  }

  private async load(id: string, maxBytes: number): Promise<MediaUploadState> {
    const state = await this.store.load(id);
    if (!state || state.id !== id) throw new Error("Media upload record is unavailable");
    const uploaded = state.status === "uploaded";
    if (!Number.isSafeInteger(state.length) || state.length < 1 || state.length > maxBytes ||
        !Number.isSafeInteger(state.offset) || state.offset < 0 || state.offset > state.length ||
        (state.status !== "pending" && !uploaded) || uploaded !== (state.offset === state.length)) {
      throw new Error("Invalid persisted media upload record");
    }
    return state;
  }
}
