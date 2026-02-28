# Postgres Message Queue (PGMQ)

[![Latest Version](https://img.shields.io/crates/v/pgmq.svg)](https://crates.io/crates/pgmq)

The Rust client for PGMQ. This gives you an ORM-like experience with the Postgres extension and makes managing connection pools, transactions, and serialization/deserialization much easier.


## Installing PGMQ

PGMQ can be installed into any existing Postgres database using this Rust client. The installation performed by the Rust
client is versioned, which means it can be used to perform a fresh installation of PGMQ, or it can upgrade an existing
installation to the latest version.

Run standard Postgres using Docker:

```bash
docker run -d -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:latest
```

### Via CLI

Install the PGMQ Rust CLI:

```bash
cargo install pgmq --features cli --bin pgmq-cli

pgmq-cli install postgres://postgres:postgres@localhost:5432/postgres
```

### In Rust

Refer to the [install example](examples/install.rs), or add PGMQ to your `Cargo.toml` with the `install-sql` feature enabled:

```bash
cargo add pgmq --features install-sql
```

```rust
async fn initialize_pgmq(pool: sqlx::Pool<sqlx::Postgres>) -> Result<(), pgmq::PgmqError> {
    let queue = pgmq::PGMQueueExt::new_with_pool(pool).await;
    queue.install_sql().await?;
    Ok(())
}
```

## Examples

The project contains several [examples](./examples/). You can run these using Cargo.

A basic example displaying the primary features:
```bash
cargo run --example basic
```

How to install PGMQ using the Rust client from within your application:

```bash
cargo run --example install --features install-sql
```

## Serialization and Deserialization

Messages can be parsed as `serde_json::Value` or into a struct of your design. `queue.read()` returns an `Result<Option<Message<T>>, PgmqError>`
where `T` is the type of the message on the queue. It returns an error when there is an issue parsing the message (`PgmqError::JsonParsingError`) or if PGMQ is unable to reach postgres (`PgmqError::DatabaseError`).

License: [PostgreSQL](LICENSE)
