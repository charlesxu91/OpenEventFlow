# Tracking Plan

The tracking plan is the source of truth for event contracts.

## Example

```json
{
  "namespace": "io.openeventflow.app",
  "schemaVendor": "io.openeventflow",
  "events": [
    {
      "name": "product_exposed",
      "version": "1-0-0",
      "owner": "recommendation",
      "description": "A product card became visible enough to count as an exposure.",
      "required": ["product_id", "page", "position", "exposure_id"],
      "properties": {
        "product_id": { "type": "string" },
        "page": { "type": "string" },
        "position": { "type": "integer" },
        "exposure_id": { "type": "string" }
      }
    }
  ]
}
```

## Generated Output

The CLI generates:

- Iglu-compatible JSON Schemas
- Kotlin event data classes
- Swift event structs

Run:

```bash
npm run codegen
```

## Compatibility

Use additive schema changes for minor event evolution:

- Add optional fields
- Keep required field names stable
- Avoid changing field types
- Introduce a new version for breaking changes
