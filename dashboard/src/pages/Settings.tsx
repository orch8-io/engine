import { useState } from "react";
import { setApiUrl, setApiKey, clearApiKey, checkHealth } from "../api";

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
      <h1 className="text-2xl font-bold">Settings</h1>

      <div className="space-y-4">
        <div>
          <label className="block text-sm text-muted mb-1">Engine API URL</label>
          <input
            type="text"
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            className="w-full bg-card border border-border rounded px-3 py-2 text-sm font-mono text-foreground"
            placeholder="http://localhost:8080"
          />
        </div>

        <div>
          <label className="block text-sm text-muted mb-1">API Key (optional)</label>
          <input
            type="password"
            value={key}
            onChange={(e) => setKey(e.target.value)}
            className="w-full bg-card border border-border rounded px-3 py-2 text-sm font-mono text-foreground"
            placeholder="Leave empty if no auth required"
          />
        </div>

        <div className="flex items-center gap-4">
          <button
            onClick={testConnection}
            className="bg-primary text-primary-foreground px-4 py-2 rounded text-sm font-medium hover:bg-primary/90 transition-colors"
          >
            Test Connection
          </button>
          {status === "ok" && <span className="text-success text-sm">{msg}</span>}
          {status === "error" && <span className="text-danger text-sm">{msg}</span>}
        </div>
      </div>
    </div>
  );
}
