Install `pgmq` using the sql-only approach. This method will perform a fresh installation if
`pgmq` is not installed, or it will upgrade `pgmq` to the latest version if it was previously
installed and there's a newer version available.

Note: This installation method should not be used if `pgmq` was installed as an actual
Postgres extension using `CREATE EXTENSION pgmq;`.
