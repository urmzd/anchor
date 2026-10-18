# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| 0.x     | Yes       |
| < 0.x   | No        |

anchor is in **beta** (pre-1.0). Only the latest 0.x release receives security fixes.

## Reporting a Vulnerability

Please report vulnerabilities privately via [GitHub Security Advisories](https://github.com/urmzd/anchor/security/advisories/new). Do not open a public issue for security reports.

You can expect an acknowledgment within 72 hours. Once a fix is available, we will coordinate disclosure with you.

Two product invariants are security boundaries. First, adapters validate and never migrate: any code path where anchor executes DDL against a user's database, or writes outside the tables a mapping declares, is a vulnerability, not a bug. Second, fencing: any way for a superseded attempt to pass the (run ID, attempt) fence on heartbeat, settle, release, or delta append is a vulnerability.
