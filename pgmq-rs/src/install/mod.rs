mod applied;
mod script;
mod version;

use crate::errors::PgmqError;
use script::MigrationScript;
use sqlx::{Pool, Postgres};

#[doc = include_str!("install_sql.md")]
pub async fn install_sql(pool: &Pool<Postgres>) -> Result<(), PgmqError> {
    let mut tx = pool.begin().await?;
    for script in MigrationScript::get_scripts(&mut tx).await? {
        script.run(&mut tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Helper method to reduce the boilerplate required to create a [`PgmqError::InstallationError`].
fn install_err(err: impl ToString) -> PgmqError {
    PgmqError::InstallationError(err.to_string())
}

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;

    #[test]
    fn install_err() {
        let err = super::install_err("Some error");
        assert_debug_snapshot!(err);
    }
}
