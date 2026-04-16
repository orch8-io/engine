// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Orch8, Inc.

import { PieceExecutionError } from "./errors";

/**
 * Minimal structural view of a loaded ActivePieces piece.
 *
 * We intentionally do NOT import types from `@activepieces/pieces-framework`
 * here — the framework's type surface drifts between major versions, and a
 * nominal import would force the sidecar to a single AP version. Duck-typing
 * keeps the adapter compatible across releases; mismatched shapes surface as
 * a clear error at invocation time rather than a compile-time trap.
 */
export interface PieceAction {
  name: string;
  displayName?: string;
  run: (ctx: unknown) => Promise<unknown>;
}

export interface Piece {
  displayName?: string;
  actions: () => Record<string, PieceAction> | PieceAction[];
  triggers?: () => Record<string, unknown> | unknown[];
}

export interface PieceLoader {
  load(pieceName: string): Promise<Piece>;
  /** Override for tests. */
  cache(pieceName: string, piece: Piece): void;
  clear(): void;
}

const SAFE_PIECE_NAME = /^[a-z0-9][a-z0-9-]*$/;

/**
 * Default loader: resolves `@activepieces/piece-<name>` via dynamic import.
 *
 * Security: the piece name arrives from the caller (orch8 engine step params
 * that originate in user-authored JSON). We validate it against a strict
 * allowlist charset before constructing the module specifier — guarding
 * against path traversal / relative specifiers that would let a malicious
 * sequence import arbitrary modules off disk.
 */
export function createDefaultLoader(options: {
  /** Optional allowlist — if set, only these piece names may be loaded. */
  allowlist?: readonly string[];
} = {}): PieceLoader {
  const cache = new Map<string, Piece>();
  const allowlist = options.allowlist ? new Set(options.allowlist) : null;

  return {
    async load(pieceName: string): Promise<Piece> {
      if (!SAFE_PIECE_NAME.test(pieceName)) {
        throw new PieceExecutionError(
          "permanent",
          `invalid piece name '${pieceName}': must match ${SAFE_PIECE_NAME}`,
        );
      }
      if (allowlist && !allowlist.has(pieceName)) {
        throw new PieceExecutionError(
          "permanent",
          `piece '${pieceName}' is not in the configured allowlist`,
        );
      }

      const cached = cache.get(pieceName);
      if (cached) return cached;

      const specifier = `@activepieces/piece-${pieceName}`;
      let mod: Record<string, unknown>;
      try {
        mod = (await import(specifier)) as Record<string, unknown>;
      } catch (err) {
        throw new PieceExecutionError(
          "permanent",
          `piece '${pieceName}' is not installed (tried '${specifier}'): ${(err as Error).message}`,
        );
      }

      const piece = findPieceExport(mod, pieceName);
      cache.set(pieceName, piece);
      return piece;
    },
    cache(pieceName, piece) {
      cache.set(pieceName, piece);
    },
    clear() {
      cache.clear();
    },
  };
}

/**
 * A piece module can export the piece under many names: default export,
 * named export matching the piece (`slack`), or just the first object that
 * looks like a piece. Walk the exports and pick the first viable one.
 */
function findPieceExport(mod: Record<string, unknown>, pieceName: string): Piece {
  const candidates: unknown[] = [
    mod.default,
    mod[pieceName],
    mod[pieceName.replace(/-/g, "_")],
    mod[toCamelCase(pieceName)],
    ...Object.values(mod),
  ];

  for (const c of candidates) {
    if (isPiece(c)) return c;
  }
  throw new PieceExecutionError(
    "permanent",
    `piece '${pieceName}' module loaded but no valid piece export found (expected an object with .actions() or .actions property)`,
  );
}

function toCamelCase(s: string): string {
  return s.replace(/-([a-z0-9])/g, (_m, c: string) => c.toUpperCase());
}

function isPiece(value: unknown): value is Piece {
  if (!value || typeof value !== "object") return false;
  const v = value as Record<string, unknown>;
  return typeof v.actions === "function" || typeof v.actions === "object";
}

/**
 * Look up an action on a loaded piece. Handles both the modern shape where
 * `actions()` returns a record keyed by action name, and the older shape
 * where it returns an array of action objects with a `.name` field.
 */
export function findAction(piece: Piece, actionName: string): PieceAction {
  const raw = typeof piece.actions === "function" ? piece.actions() : piece.actions;

  if (Array.isArray(raw)) {
    const found = raw.find((a) => a.name === actionName);
    if (found) return found;
  } else if (raw && typeof raw === "object") {
    const rec = raw as Record<string, PieceAction>;
    if (rec[actionName]) return rec[actionName];
    // Some pieces key by the action's display identifier, not `name`.
    for (const a of Object.values(rec)) {
      if (a.name === actionName) return a;
    }
  }

  throw new PieceExecutionError(
    "permanent",
    `action '${actionName}' not found on piece`,
  );
}
