# SaaS PostgreSQL operations

## Fresh server and migration

`saas/ops/vps-setup.sh` installs loopback-only PostgreSQL, creates the
`actae_saas` role/database, stores its password in a mode-0600 pgpass file,
sets a five-connection application pool, and enables logical backups plus PITR
archives. On a legacy installation it refuses to continue until SQLite is
migrated explicitly:

```bash
sudo -u actae /opt/actae-saas/venv/bin/python -m actae_saas migrate-postgres \
  --sqlite /var/lib/actae-saas/saas.db \
  --postgres 'postgresql://actae_saas@127.0.0.1:5432/actae_saas?sslmode=disable'
```

The target must be empty. The command copies in foreign-key order, verifies
every table count, commits only after all checks, and refuses a second run.
Keep the SQLite file read-only until the PostgreSQL acceptance test passes.

## Backup layers

- Nightly `pg_dump` custom archive, retained seven days and verified before
  publication.
- Weekly restore into an isolated throwaway database, with schema/application
  invariants.
- Continuous WAL archive with five-minute archive timeout and two physical
  base backups. `PITR_RSYNC_TARGET` must point to another failure domain.
- `/etc/actae-saas.env`, `.pgpass`, signing key, and Fernet key need a separate
  encrypted secret backup; database recovery alone cannot decrypt key material.

Test both a latest restore and a point-in-time restore quarterly. For PITR,
stop writes, select the newest base backup before the target timestamp, extract
it into a new PostgreSQL data directory, create `recovery.signal`, configure
`restore_command` to copy `%f` from the off-host WAL archive, set
`recovery_target_time`, and start on an isolated port. Verify customer/install
counts and a known pre-target record, then promote and switch the application
DSN. Never restore over the only production cluster.

## HA boundary

The included configuration is recoverable single-node PostgreSQL, not HA. GA
requires a managed multi-AZ PostgreSQL service or a separately operated
primary/standby cluster with a stable failover endpoint. Test forced primary
loss while API processes are active, pool reconnection, zero split-brain, and
RPO/RTO. A replica on the same VPS is not another failure domain.
