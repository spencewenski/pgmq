use clap::{Parser, Subcommand};
use pgmq::install::install_sql;
use pgmq::PgmqError;
use serde::Serialize;
use sqlx::PgPool;
use url::Url;

#[derive(Debug, Parser)]
#[clap(author, version, about = "PGMQ CLI tool for installing and managing PostgreSQL message queues", long_about = None)]
pub struct Arguments {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Install PGMQ into a PostgreSQL database using the sql-only installation approach
    Install(InstallArgs),
}

#[derive(Debug, Parser, Serialize)]
#[non_exhaustive]
pub struct InstallArgs {
    /// PostgreSQL connection URL
    pub database_url: Url,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), PgmqError> {
    let args = Arguments::parse();

    match args.command {
        Commands::Install(args) => {
            let pool = PgPool::connect(args.database_url.as_str())
                .await
                .expect("Failed to connect to database");

            install_sql(&pool).await?;
        }
    }

    Ok(())
}
