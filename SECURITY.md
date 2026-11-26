# Security policy

## Supported versions

The latest minor release receives security fixes. Pre-1.0, only `main` and the
latest tag are supported.

## Reporting a vulnerability

Use GitHub private vulnerability reporting:

https://github.com/actae-dev/actae/security/advisories/new

If that is unavailable, email security@actae.dev with:

- affected version or commit,
- a description and impact,
- reproduction steps or a proof of concept,
- whether you want public credit.

Do not open a public issue, and do not test against infrastructure you do not own.

## What to expect

| Step | Target |
|---|---|
| Acknowledgement | 2 business days |
| Initial assessment | 5 business days |
| Fix or mitigation plan | 30 days for high/critical |
| Public disclosure | coordinated, after a fix ships |

We credit reporters in the advisory unless asked not to.

## Scope

In scope: the runtime, dashboard, SDKs, install scripts, release artifacts.

Out of scope: the hosted service, third-party dependencies (report upstream),
social engineering, denial of service through intentional resource exhaustion.

## Safe harbor

Good-faith research on your own installation is welcome. We will not pursue
legal action for research that follows this policy and avoids privacy
violations, data destruction and service disruption.
