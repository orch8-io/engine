export interface EngineConnection {
  baseUrl: string;
  apiKey: string | null;
  tenantId: string | null;
}

const CONNECTION_KEY = "orch8_engine_connection";
const LEGACY_KEYS = ["orch8_api_url", "orch8_api_key", "orch8_tenant_id"] as const;
type ConnectionStorage = Pick<Storage, "getItem" | "setItem" | "removeItem">;

export function engineUrl(value: string): URL {
  const url = new URL(value.trim());
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) {
    throw new Error("Enter an HTTP or HTTPS engine URL without credentials.");
  }
  return url;
}

export function readEngineConnection(storage: ConnectionStorage, defaultUrl: string): EngineConnection {
  const stored = storage.getItem(CONNECTION_KEY);
  if (stored === null) {
    const legacyBaseUrl = storage.getItem(LEGACY_KEYS[0]);
    return {
      baseUrl: legacyBaseUrl || defaultUrl,
      apiKey: legacyBaseUrl ? storage.getItem(LEGACY_KEYS[1]) : null,
      tenantId: legacyBaseUrl ? storage.getItem(LEGACY_KEYS[2]) : null,
    };
  }
  try {
    const profile = JSON.parse(stored) as Partial<EngineConnection> & { origin?: unknown };
    if (typeof profile?.baseUrl === "string" && engineUrl(profile.baseUrl).origin === profile.origin) {
      return {
        baseUrl: profile.baseUrl,
        apiKey: typeof profile.apiKey === "string" ? profile.apiKey : null,
        tenantId: typeof profile.tenantId === "string" ? profile.tenantId : null,
      };
    }
  } catch {
    // Fall back to an unauthenticated connection if the profile is invalid.
  }
  return { baseUrl: defaultUrl, apiKey: null, tenantId: null };
}

export function saveEngineConnection(storage: ConnectionStorage, connection: EngineConnection) {
  const url = engineUrl(connection.baseUrl);
  storage.setItem(CONNECTION_KEY, JSON.stringify({ ...connection, baseUrl: url.href, origin: url.origin }));
  for (const key of LEGACY_KEYS) storage.removeItem(key);
}
