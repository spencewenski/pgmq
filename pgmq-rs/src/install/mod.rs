mod applied;
mod script;
mod version;

use crate::errors::PgmqError;
use script::MigrationScript;
use sqlx::{Pool, Postgres};

#[doc = include_str!("install_sql.md")]
pub async fn install_sql(pool: &Pool<Postgres>) -> Result<(), PgmqError> {
    for script in MigrationScript::get_scripts(pool).await? {
        script.run(pool).await?;
    }

    Ok(())
}

/// Helper method to reduce the boilerplate required to create a [`PgmqError::InstallationError`].
fn install_err(err: impl ToString) -> PgmqError {
    PgmqError::InstallationError(err.to_string())
}
