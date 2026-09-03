# MCP authoring

The server exposes `/api/v1/mcp` with eleven tools, including
`create_sequence`, `preflight_sequence`, and `lint_sequence`. It uses the same
API key and tenant boundary as REST.

Claude Desktop configuration:

```json
{
  "mcpServers": {
    "orch8": {
      "url": "http://localhost:8080/api/v1/mcp",
      "headers": { "x-api-key": "${ORCH8_API_KEY}", "x-tenant-id": "demo" }
    }
  }
}
```

Cursor uses the same object in `.cursor/mcp.json`. Keep credentials in an
environment-expanded user configuration; do not commit literal keys.

An authoring agent should call `lint_sequence`, repair all findings, call
`preflight_sequence`, and only then call `create_sequence`. The CLI provides the
same bounded loop as `orch8 generate` for OpenAI-compatible providers.
