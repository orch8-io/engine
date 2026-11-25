import { useState } from "react";
import { getConnection, saveConnection, checkHealth } from "../api";
import { engineUrl } from "../lib/connection";
import { usePageTitle } from "../hooks/usePageTitle";
import { PageHeader } from "../components/ui/PageHeader";
import { Section } from "../components/ui/Section";
import { Button } from "../components/ui/Button";
import { Input, FieldLabel } from "../components/ui/Input";
import { StatusDot } from "../components/ui/StatusDot";

export default function Settings() {
  usePageTitle("Settings");
  const [draft, setDraft] = useState(getConnection);
  const [status, setStatus] = useState<"idle" | "checking" | "ok" | "error">("idle");
  const [msg, setMsg] = useState("");

  function changeUrl(baseUrl: string) {
    setStatus("idle");
    setDraft((previous) => {
      try {
        if (engineUrl(baseUrl).origin === engineUrl(previous.baseUrl).origin) {
          return { ...previous, baseUrl };
        }
      } catch {
        // An incomplete address is no longer a match for the credential's origin.
      }
      return { baseUrl, apiKey: null, tenantId: null };
    });
  }

  async function testConnection() {
    setStatus("checking");
    try {
      await checkHealth(undefined, draft.baseUrl);
      saveConnection(draft);
      setStatus("ok");
      setMsg("Saved. Engine is reachable.");
    } catch (e: unknown) {
      setStatus("error");
      setMsg(e instanceof Error ? e.message : "Connection failed");
    }
  }

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Settings"
        description="Choose the engine for this console. Connection settings are stored in your browser."
      />

      <Section
        eyebrow="Connection"
        title="Engine endpoint"
        description={
          <>
            Point this console at any orch8 engine HTTP API. For local
            development that's usually{" "}
            <code className="font-mono text-ink">http://localhost:8080</code>.
            The <strong className="text-ink">API key</strong> is only
            required if the engine has auth enabled — leave empty otherwise.
          </>
        }
        annotation={
          <>
            <strong className="text-ink">Storage.</strong> Values are saved
            in <code className="font-mono">localStorage</code> on this
            device only. Clearing your browser data removes them.
            <br />
            <br />
            <strong className="text-ink">Auth.</strong> The API key is sent
            as the <code className="font-mono">X-API-Key</code>{" "}
            header and the optional tenant as <code className="font-mono">X-Tenant-Id</code>{" "}
            on JSON API requests. Never paste a production key into a shared machine.
          </>
        }
      >
        <div className="max-w-[520px] space-y-6">
          <div>
            <FieldLabel htmlFor="engine-api-url">Engine API URL</FieldLabel>
            <Input
              id="engine-api-url"
              type="text"
              value={draft.baseUrl}
              onChange={(e) => changeUrl(e.target.value)}
              disabled={status === "checking"}
              className="w-full font-mono"
              placeholder="http://localhost:8080"
            />
            <p className="annotation mt-1.5">
              The base URL — no trailing slash. The console will append
              paths like <code className="font-mono">/health</code>,{" "}
              <code className="font-mono">/instances</code>.
            </p>
          </div>

          <div>
            <FieldLabel htmlFor="engine-api-key">API key</FieldLabel>
            <Input
              id="engine-api-key"
              type="password"
              autoComplete="new-password"
              value={draft.apiKey ?? ""}
              onChange={(e) => {
                setDraft({ ...draft, apiKey: e.target.value || null });
                setStatus("idle");
              }}
              disabled={status === "checking"}
              className="w-full font-mono"
              placeholder="Leave empty if no auth required"
            />
            <p className="annotation mt-1.5">
              Enter a new key when switching engines.
            </p>
          </div>

          <div>
            <FieldLabel htmlFor="engine-tenant-id">Tenant ID</FieldLabel>
            <Input
              id="engine-tenant-id"
              type="text"
              value={draft.tenantId ?? ""}
              onChange={(e) => {
                setDraft({ ...draft, tenantId: e.target.value || null });
                setStatus("idle");
              }}
              disabled={status === "checking"}
              className="w-full font-mono"
              placeholder="Required when tenant headers are enforced"
            />
            <p className="annotation mt-1.5">
              Scopes dashboard JSON API requests when <code className="font-mono">ORCH8_REQUIRE_TENANT_HEADER</code> is enabled.
            </p>
          </div>

          <div className="flex items-center gap-3 pt-1">
            <Button variant="primary" onClick={testConnection} disabled={status === "checking"}>
              {status === "checking" ? "Checking…" : "Test connection and save"}
            </Button>
            {status === "ok" && (
              <span role="status" className="flex items-center gap-1.5 text-ok text-[12px] font-mono uppercase tracking-wider">
                <StatusDot tone="ok" live />
                {msg}
              </span>
            )}
            {status === "error" && (
              <span role="alert" className="flex items-center gap-1.5 text-warn text-[12px] font-mono uppercase tracking-wider">
                <StatusDot tone="warn" />
                {msg}
              </span>
            )}
          </div>
          <p className="annotation">
            The connection test checks <code className="font-mono">/health/live</code>{" "}
            before saving. API requests verify your credentials when you open an engine page.
          </p>
        </div>
      </Section>
    </div>
  );
}
