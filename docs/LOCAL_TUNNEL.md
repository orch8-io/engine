# Receive local webhooks

Orch8 intentionally does not ship a proprietary reverse-tunnel service. Use a
maintained tunnel and point the provider at Orch8's signed webhook route.

```bash
orch8 dev .
ngrok http 8080
# or: cloudflared tunnel --url http://localhost:8080
```

Configure the public URL as
`https://<tunnel>/api/v1/webhooks/<slug>`. Always configure the trigger secret;
the engine verifies signatures and replay windows even when the tunnel URL is
public. Tunnel URLs are for development, never the production availability or
identity boundary.
