# Independent security review scope

Commission an assessor who was not responsible for implementation. Scope the
SaaS portal/API, OAuth and password reset, Polar webhooks and plan mapping,
license/enrollment/key lifecycle, tenant ingress/network/container boundaries,
worker privilege, PostgreSQL/R2/restic recovery, release signing/update, and
account deletion.

Provide architecture/data-flow diagrams, threat model, dependency inventories,
test accounts, a staging node, source at the reviewed commit, and restore/key
rotation runbooks. Require reproducible findings with severity and affected
commit. GA acceptance is a signed report with no open critical/high findings
and a retest letter. Internal tests and dependency scanners are useful inputs,
but are not an independent review.
