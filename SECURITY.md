# Security Policy

OpenEventFlow handles behavioral event data and should be treated as data infrastructure.

## Reporting

Please report security issues privately to the maintainers before public disclosure. Include:

- Affected component
- Impact
- Reproduction steps
- Suggested mitigation, if known

## Sensitive Data

Do not send passwords, secrets, payment card data, government identifiers, or unapproved PII through event properties. Use privacy contexts and field-level redaction rules before events leave the device.
