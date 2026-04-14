import { useState } from "react";
import { setApiUrl, setApiKey, clearApiKey, checkHealth } from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { Section } from "../components/ui/Section";
import { Button } from "../components/ui/Button";
import { Input, FieldLabel } from "../components/ui/Input";
import { StatusDot } from "../components/ui/StatusDot";

export default function Settings() {
  const [url, setUrl] = useState(
    localStorage.getItem("orch8_api_url") ||
      import.meta.env.VITE_ORCH8_API_URL ||
      "http://localhost:8080",
  );
  const [key, setKey] = useState(localStorage.getItem("orch8_api_key") || "");
  const [status, setStatus] = useState<"idle" | "ok" | "error">("idle");
  const [msg, setMsg] = useState("");

  async function testConnection() {
    setStatus("idle");
    try {
      if (url) setApiUrl(url);
      if (key) setApiKey(key);
      else clearApiKey();

      await checkHealth();
      setStatus("ok");
      setMsg("Connected");
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
        description="Where the console lives: which engine it talks to and what credentials it presents. Settings are stored in your browser's local storage — nothing is sent to any third party."
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
            as the <code className="font-mono">Authorization: Bearer ...</code>{" "}
            header on every request. Never paste a production key into a
            shared machine.
          </>
        }
      >
        <div className="max-w-[520px] space-y-6">
          <div>
            <FieldLabel>Engine API URL</FieldLabel>
            <Input
              type="text"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
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
            <FieldLabel>API key</FieldLabel>
            <Input
              type="password"
              value={key}
              onChange={(e) => setKey(e.target.value)}
              className="w-full font-mono"
              placeholder="Leave empty if no auth required"
            />
            <p className="annotation mt-1.5">
              Optional. Sent as a bearer token on every request.
            </p>
          </div>

          <div className="flex items-center gap-3 pt-1">
            <Button variant="primary" onClick={testConnection}>
              Test connection
            </Button>
            {status === "ok" && (
              <span className="flex items-center gap-1.5 text-ok text-[12px] font-mono uppercase tracking-wider">
                <StatusDot tone="ok" live />
                {msg}
              </span>
            )}
            {status === "error" && (
              <span className="flex items-center gap-1.5 text-warn text-[12px] font-mono uppercase tracking-wider">
                <StatusDot tone="warn" />
                {msg}
              </span>
            )}
          </div>
          <p className="annotation">
            “Test connection” pings <code className="font-mono">/health</code>{" "}
            with your current settings. A success means the console can
            reach the engine and authenticate — not that the engine itself
            is fully healthy. Use the Overview page for that.
          </p>
        </div>
      </Section>
    </div>
  );
}
