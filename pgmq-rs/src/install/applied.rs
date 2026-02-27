use crate::install::script::MigrationScript;
use crate::install::version::Version;
use crate::PgmqError;
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::{Acquire, Pool, Postgres, Row};
use std::str::FromStr;

/// Struct to represent a row of the DB table that tracks which migration scripts have been applied.
pub struct AppliedMigration {
    /// The name of the migration script.
    pub name: String,
    /// The version of `pgmq` after the migration script was applied. Mainly useful to record
    /// which version was installed by the `pgmq.sql` script for a fresh installation -- this
    /// script does not embed the `pgmq` version in its name, unlike the other migration scripts.
    pub version: Version,
}

impl AppliedMigration {
    /// Create the DB table used to keep track of which migration scripts have been applied.
    pub async fn create_table(pool: &Pool<Postgres>) -> Result<(), PgmqError> {
        let mut tx = pool.begin().await?;

        // Because this may run before `pgmq` has been installed, we need to ensure the `pgmq`
        // DB schema is created.
        sqlx::query("CREATE SCHEMA IF NOT EXISTS pgmq;")
            .execute(tx.acquire().await?)
            .await?;

        sqlx::query(
        "CREATE TABLE IF NOT EXISTS pgmq.__pgmq_migrations ( name TEXT PRIMARY KEY NOT NULL, version TEXT NOT NULL, run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP);",
        )
        .execute(tx.acquire().await?)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    /// Fetch all of the migrations that were previously applied.
    pub async fn fetch_all(pool: &Pool<Postgres>) -> Result<Vec<AppliedMigration>, PgmqError> {
        let applied_migrations = sqlx::query("SELECT name, version FROM pgmq.__pgmq_migrations")
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| -> Result<AppliedMigration, PgmqError> {
                Ok(Self {
                    name: row.get::<String, _>("name"),
                    version: Version::from_str(&row.get::<String, _>("version"))?,
                })
            })
            .collect::<Result<Vec<AppliedMigration>, PgmqError>>()?;
        Ok(applied_migrations)
    }

    /// Record that the provided [`MigrationScript`] was applied.
    pub fn insert_script(
        script: &'_ MigrationScript,
    ) -> Result<Query<'_, Postgres, PgArguments>, PgmqError> {
        let query =
            sqlx::query("INSERT INTO pgmq.__pgmq_migrations ( name, version ) VALUES ( $1, $2 );")
                .bind(script.name.original)
                .bind(script.name.to.to_string());
        Ok(query)
    }
}
