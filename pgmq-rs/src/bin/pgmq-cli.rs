use clap::{Arg, Command};

use pgmq::install::install_sql;
use sqlx::PgPool;
use std::process;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let matches = Command::new("pgmq-cli")
        .about("PGMQ CLI tool for installing and managing PostgreSQL message queues")
        .subcommand(
            Command::new("install")
                .about("Install PGMQ into a PostgreSQL database using the sql-only installation approach")
                .arg(
                    Arg::new("database_url")
                        .help("PostgreSQL connection URL")
                        .required(true)
                        .index(1),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("install", sub_matches)) => {
            let database_url = sub_matches
                .get_one::<String>("database_url")
                .expect("Unable to fetch db url from args");

            let pool = PgPool::connect(database_url)
                .await
                .expect("Failed to connect to database");

            if let Err(e) = install_sql(&pool).await {
                process::exit(1);
            }
        }
        _ => {
            log::error!("No valid subcommand provided. Use --help for usage information.");
            // process::exit(1);
        }
    }
}
