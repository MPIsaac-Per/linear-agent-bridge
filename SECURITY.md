# Security Policy

## Reporting Security Vulnerabilities

Please **do not** open public GitHub issues for security vulnerabilities.

If you discover a security issue, report it privately via [GitHub Private Vulnerability Reporting](https://github.com/MPIsaac-Per/linear-agent-bridge/security/advisories/new) or contact the maintainers directly.

## Unattended Runtime Threat Model

Linear Agent Bridge executes agent commands on host environments. Keep the following security principles in mind:

- **Credential Isolation**: Never expose Linear API tokens, webhook secrets, or private keys to untrusted agent runtime prompts.
- **Process Boundaries**: Agent execution commands should run in controlled, non-root workspaces.
- **Webhook Signature Verification**: All incoming webhooks must be cryptographically verified against the configured signing secret before job dispatch.
