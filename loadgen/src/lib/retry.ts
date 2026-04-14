/**
 * Tiny retry helper for HTTP calls.
 *
 * Three attempts, jittered exponential backoff. Any ApiError with 4xx
 * (except 429) short-circuits — those are programmer errors, not
 * transient.
 */

import { ApiError } from "../client.ts";
import { sleep } from "./clock.ts";

export interface RetryResult<T> {
  value?: T;
  error?: unknown;
}

export async function withRetry<T>(
  label: string,
  fn: () => Promise<T>,
  attempts: number = 3,
): Promise<T> {
  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (err instanceof ApiError) {
        if (err.status >= 400 && err.status < 500 && err.status !== 429) {
          throw err;
        }
      }
      if (i === attempts - 1) break;
      const base = 100 * Math.pow(4, i);
      const jitter = Math.floor(Math.random() * base * 0.25);
      await sleep(base + jitter);
    }
  }
  const msg = lastErr instanceof Error ? lastErr.message : String(lastErr);
  throw new Error(`[${label}] failed after ${attempts} attempts: ${msg}`);
}
