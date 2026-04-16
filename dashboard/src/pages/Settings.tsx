import { useState } from "react";
import { setApiUrl, setApiKey, clearApiKey, checkHealth } from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { Panel, PanelBody } from "../components/ui/Panel";
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
    <div className="max-w-lg space-y-6">
      <PageHeader
        eyebrow="Operator"
        title="Settings"
        description="Point the console at an engine and (optionally) authenticate."
      />

      <Panel>
        <PanelBody>
          <div className="space-y-4">
            <div>
              <FieldLabel>Engine API URL</FieldLabel>
              <Input
                type="text"
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                className="w-full font-mono"
                placeholder="http://localhost:8080"
              />
            </div>

            <div>
              <FieldLabel>API Key</FieldLabel>
              <Input
                type="password"
                value={key}
                onChange={(e) => setKey(e.target.value)}
                className="w-full font-mono"
                placeholder="Leave empty if no auth required"
              />
            </div>

            <div className="flex items-center gap-3 pt-1">
              <Button variant="primary" onClick={testConnection}>
                Test connection
              </Button>
              {status === "ok" && (
                <span className="flex items-center gap-1.5 text-ok text-[13px]">
                  <StatusDot tone="ok" live />
                  {msg}
                </span>
              )}
              {status === "error" && (
                <span className="flex items-center gap-1.5 text-warn text-[13px]">
                  <StatusDot tone="warn" />
                  {msg}
                </span>
              )}
            </div>
          </div>
        </PanelBody>
      </Panel>
    </div>
  );
}
