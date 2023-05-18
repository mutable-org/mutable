# PostgreSQL Connector

## Configuration of the PostgreSQL Server

Our benchmarking workload frequently (re-)creates tables and drops them again.
This seems to put a lot of pressure on PostgreSQL's WAL archiving.
See [this article](https://dzone.com/articles/postgresql-why-and-how-wal-bloats) for a detailed explanation and analysis.
To work around this, we must configure PostgreSQL to minimize WAL bloat and overhead.
The following suggested settings are taken from [this SO post](https://dba.stackexchange.com/questions/103148/is-it-possible-to-run-postgres-with-no-wal-files-being-produced).

### Apply Settings

Locate the configuration file of PostgreSQL:

```
$ psql -c 'SHOW config_file;'
```

In my case, the location is `/var/lib/postgres/data/postgresql.conf`.

Edit the file by commenting out the respective settings and then changing their value.
Apply the following settings.
You can find an explanation of these settings in [the PostgreSQL documentation](https://www.postgresql.org/docs/current/runtime-config-wal.html).

- `wal_level = minimal`
- `fsync = off`
- `synchronous_commit = off`
- `full_page_writes = off`
- `max_wal_senders = 0`
- `archive_mode = off`


We can justify our settings with the following statement, taken from [the PostgreSQL documentation](https://www.postgresql.org/docs/current/runtime-config-wal.html):
> Examples of safe circumstances for turning off `fsync` include the initial loading of a new database cluster from a backup file, using a database cluster for processing a batch of data after which the database will be thrown away and recreated, or for a read-only database clone which gets recreated frequently and is not used for failover.

## Tweaks in our PostgreSQL Database Connector

We will only create `UNLOGGED` tables for our benchmarks.
The [PostgreSQL documentation for `UNLOGGED`](https://www.postgresql.org/docs/current/sql-createtable.html) says
> If specified, the table is created as an unlogged table. Data written to unlogged tables is not written to the write-ahead log (see Chapter 30), which makes them considerably faster than ordinary tables. However, they are not crash-safe: an unlogged table is automatically truncated after a crash or unclean shutdown. The contents of an unlogged table are also not replicated to standby servers. Any indexes created on an unlogged table are automatically unlogged as well.
>
> If this is specified, any sequences created together with the unlogged table (for identity or serial columns) are also created as unlogged.


