// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use deadpool_postgres::Pool;
use tracing::info;

struct Migration {
    version: i32,
    name: &'static str,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "create_users",
        sql: r#"
CREATE TABLE IF NOT EXISTS public.user (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    "emailVerified" BOOLEAN NOT NULL DEFAULT false,
    name TEXT NOT NULL DEFAULT '',
    image TEXT,
    password_hash TEXT,
    "createdAt" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "updatedAt" TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$ BEGIN
    ALTER TABLE public.user ADD COLUMN IF NOT EXISTS password_hash TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS public.session (
    id TEXT PRIMARY KEY,
    expiresAt TIMESTAMPTZ NOT NULL,
    token TEXT NOT NULL,
    userId TEXT NOT NULL REFERENCES public.user(id) ON DELETE CASCADE,
    ipAddress TEXT,
    userAgent TEXT,
    createdAt TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedAt TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_session_token ON public.session(token);
CREATE INDEX IF NOT EXISTS idx_session_userId ON public.session(userId);

CREATE TABLE IF NOT EXISTS public.api_keys (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES public.user(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    key_hash TEXT NOT NULL,
    key_prefix TEXT NOT NULL DEFAULT '',
    permissions JSONB NOT NULL DEFAULT '{}',
    last_used_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_user ON public.api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON public.api_keys(key_hash);
        "#,
    },
    Migration {
        version: 4,
        name: "drop_tenants",
        sql: r#"
DROP TABLE IF EXISTS public.tenants;
        "#,
    },
    Migration {
        version: 5,
        name: "api_keys_org_default",
        sql: r#"
DO $$ BEGIN
    ALTER TABLE public.api_keys
        ALTER COLUMN organization_id SET DEFAULT gen_random_uuid();
EXCEPTION WHEN undefined_table OR undefined_column THEN NULL;
END $$;
        "#,
    },
    Migration {
        version: 6,
        name: "control_plane",
        sql: r#"
-- Key origin: 'bootstrap' (operator-managed, exempt from subscription
-- enforcement) or 'control_plane' (synced from the SaaS control plane).
-- Existing/legacy keys default to 'bootstrap' so upgrades never silently
-- lose manually provisioned keys; control-plane keys are always written
-- with an explicit source.
ALTER TABLE public.api_keys ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'bootstrap';
ALTER TABLE public.api_keys ADD COLUMN IF NOT EXISTS key_prefix TEXT NOT NULL DEFAULT '';

-- Control-plane keys carry a SaaS customer id in user_id that has no
-- corresponding row in public.user — the FK (and its ON DELETE CASCADE)
-- is a legacy of the removed admin provisioning flow.
DO $$ BEGIN
    ALTER TABLE public.api_keys DROP CONSTRAINT IF EXISTS api_keys_user_id_fkey;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Single-row subscription state persisted by the control-plane sync loop.
-- status: 'active' | 'suspended' | 'cancelled' | 'unknown'
CREATE TABLE IF NOT EXISTS public.subscription_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    status TEXT NOT NULL DEFAULT 'unknown',
    last_sync_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    rev BIGINT NOT NULL DEFAULT 0,
    last_error TEXT
);
        "#,
    },
    Migration {
        version: 7,
        name: "api_keys_id_text",
        sql: r#"
-- The docker seed historically created api_keys.id as UUID while the app
-- schema uses TEXT (both are just opaque key-row identifiers). Normalize
-- existing UUID-schema tables so bootstrap imports and key lookups work
-- identically everywhere. Fresh databases already use TEXT and skip this.
ALTER TABLE public.api_keys ALTER COLUMN id TYPE TEXT USING id::text;
        "#,
    },
    Migration {
        version: 8,
        name: "preferences",
        sql: r#"
-- User preferences: dashboard filter/layout persistence, keyed by the
-- resolved identity string (JWT subject or api_keys.user_id). Opaque
-- JSONB — the server only enforces shape (a JSON object) and size.
CREATE TABLE IF NOT EXISTS public.preferences (
    user_id TEXT PRIMARY KEY,
    prefs JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
        "#,
    },
    Migration {
        version: 9,
        name: "api_keys_source_default_local",
        sql: r#"
-- The `bootstrap` key source was removed (no operator-managed exemption).
-- Every insert now specifies `source` explicitly ('control_plane' synced
-- keys, 'local' host-created keys); flip the column default so a bare
-- INSERT (e.g. the ApiKeyRepository test helper) cannot accidentally mint
-- a key tagged with the removed source.
ALTER TABLE public.api_keys ALTER COLUMN source SET DEFAULT 'local';
        "#,
    },
    Migration {
        version: 10,
        name: "dashboard_sso_tokens",
        sql: r#"
-- Single-use nonce ledger for dashboard SSO handoff tokens. A signed
-- handoff token (minted by the control plane, exchanged at
-- POST /api/v1/auth/sso) carries a `jti`; recording it here makes the
-- token one-time, so a replayed URL from browser history or a proxy log
-- is rejected. Rows are pruned opportunistically once expired.
CREATE TABLE IF NOT EXISTS public.dashboard_sso_tokens (
    jti TEXT PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dashboard_sso_tokens_expiry
    ON public.dashboard_sso_tokens(expires_at);
        "#,
    },
    Migration {
        version: 11,
        name: "dashboard_sessions",
        sql: r#"
-- Server-side dashboard session ledger. Every session JWT carries a `jti`; the
-- row here makes the session revocable (logout, "sign out everywhere", and a
-- password reset all revoke). `auth_method` records how it was minted
-- (password | sso | api_key). Expired/revoked rows are pruned opportunistically.
CREATE TABLE IF NOT EXISTS public.dashboard_sessions (
    jti TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    auth_method TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_dashboard_sessions_user
    ON public.dashboard_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_dashboard_sessions_expiry
    ON public.dashboard_sessions(expires_at);
        "#,
    },
    Migration {
        version: 12,
        name: "instance_setup",
        sql: r#"
-- First-run setup state for a standalone instance. When the initial admin is
-- created without a preset password, a one-time setup token (stored hashed)
-- lets the operator set the admin password from the dashboard before the
-- account can be used. `setup_complete` flips true once set.
CREATE TABLE IF NOT EXISTS public.instance_setup (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    setup_complete BOOLEAN NOT NULL DEFAULT FALSE,
    token_hash TEXT,
    token_expires_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO public.instance_setup (singleton) VALUES (TRUE) ON CONFLICT DO NOTHING;
-- An upgraded install that already has accounts is already set up.
UPDATE public.instance_setup SET setup_complete = TRUE
WHERE setup_complete = FALSE AND EXISTS (SELECT 1 FROM public.user);
        "#,
    },
    Migration {
        version: 13,
        name: "user_role",
        sql: r#"
-- Dashboard roles: `admin` can manage users, `member` is read/write on
-- channels. The earliest existing account (the standalone first-boot admin on
-- a pre-existing install) is promoted to admin; new signups default to member.
ALTER TABLE public.user ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'member';
UPDATE public.user SET role = 'admin'
WHERE id = (SELECT id FROM public.user ORDER BY "createdAt" ASC LIMIT 1);
        "#,
    },
];

pub async fn run_client_migrations(pool: &Pool) -> Result<(), anyhow::Error> {
    let client = pool.get().await?;

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS _client_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            &[],
        )
        .await?;

    let row = client
        .query_one(
            "SELECT COALESCE(MAX(version), 0) AS max_version FROM _client_migrations",
            &[],
        )
        .await?;

    let current_version: i32 = row.get("max_version");

    let mut applied = 0;
    for migration in MIGRATIONS {
        if migration.version > current_version {
            client.batch_execute(migration.sql).await?;

            client
                .execute(
                    "INSERT INTO _client_migrations (version, name) VALUES ($1, $2)",
                    &[&migration.version, &migration.name],
                )
                .await?;

            applied += 1;
        }
    }

    if applied > 0 {
        info!("Applied {} client migrations", applied);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migrations_count() {
        assert!(MIGRATIONS.len() >= 2);
    }

    #[test]
    fn test_migrations_unique_versions() {
        let mut versions: Vec<i32> = MIGRATIONS.iter().map(|m| m.version).collect();
        let original_len = versions.len();
        versions.sort_unstable();
        versions.dedup();
        assert_eq!(versions.len(), original_len, "Duplicate migration versions");
    }

    #[test]
    fn test_migrations_sequential() {
        let mut prev: i32 = 0;
        for m in MIGRATIONS {
            assert!(
                m.version > prev,
                "Migrations must be monotonically increasing: {} <= {}",
                m.version,
                prev
            );
            prev = m.version;
        }
    }

    #[test]
    fn test_migrations_non_empty() {
        for m in MIGRATIONS {
            assert!(!m.name.is_empty());
            assert!(!m.sql.trim().is_empty());
        }
    }

    #[test]
    fn test_migration_construction() {
        let m = Migration {
            version: 99,
            name: "test",
            sql: "SELECT 1",
        };
        assert_eq!(m.version, 99);
        assert_eq!(m.name, "test");
    }
}
