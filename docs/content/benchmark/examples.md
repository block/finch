---
weight: 2
---

Finch ships with example benchmarks in subdirectory `benchmarks/`.
As complete working examples that range from simple to complex, they're good study material for learning how to write Finch benchmarks and use Finch features.

A quick run set of commands is included that presumes:
* Current working directly in repo: `bin/finch/`
* [Default MySQL user]({{< relref "operate/mysql#user" >}})
* Database `finch` exists and is empty (no tables)

{{< hint type=note title="Default Database" >}}
The Finch example benchmarks do not have a default database, and they do not drop schemas or tables.
Create a database to use&mdash;database `finch` is suggested.
Drop and recreate the database to reset and rerun a benchmark.
{{< /hint >}}

{{< toc >}}

## aurora

Original 2015 Amazon Aurora benchmark
{.tagline}

|Stage|Type|Description|
|-----|----|-----------|
|setup.yaml|DDL|Create schema and insert rows|
|write-only.yaml|Standard|Execute read-write transaction|

A short benchmark with a [long and complicated history](https://hackmysql.com/post/are-aurora-performance-claims-true/).
The short version: Amazon used this benchmark in 2015 to established its "5X greater throughput than MySQL" claim.
Under the hood, it's the sysbench write-only benchmark with a specific workload: 4 EC2 (compute) instances each running 1,000 clients (all in the same availability zone), querying 250 tables each with 25,0000 rows.

Since this benchmark was designed to run on multiple compute instances, it's a little difficult to run locally unless you scale down the instances and clients:

```bash
finch -D finch -p instances=1 -p clients=8 benchmarks/aurora/write-only.yaml
```

## intro

[Intro / Start Here]({{< relref "intro/start-here" >}}) benchmarks
{.tagline}

|Stage|Type|Description|
|-----|----|-----------|
|setup.yaml|DDL|Create schema and insert rows|
|read-only.yaml|Standard|Execute single SELECT|
|row-lock.yaml|Standard|Execute SELECT and UPDATE transaction on 1,000 rows|

Quick run:

```sh
./finch -D finch ../../benchmarks/intro/setup.yaml 
# Takes awhile to insert 100,000 rows

./finch -D finch ../../benchmarks/intro/read-only.yaml
# Runs for 10s

./finch -D finch ../../benchmarks/intro/row-lock.yaml
# Runs for 20s
```

Demonstrates very basic Finch benchmarks and other concepts (like [stats output]({{< ref "benchmark/statistics" >}})).
Queries use `finch.t1` explicitly, so benchmark must use database `finch`.

## sysbench

sysbench benchmarks recreated in Finch
{.tagline}

|Stage|Type|Description|
|-----|----|-----------|
|setup.yaml|DDL|Create schema and insert rows|
|read-only.yaml|Standard|sysbench OLTP read-only benchmark|
|write-only.yaml|Standard|sysbench OLTP write-only benchmark|

Quick run:

```sh
./finch -D finch ../../benchmarks/sysbench/setup.yaml 

./finch -D finch ../../benchmarks/sysbench/read-only.yaml
```

These recreate two of the legendary [sysbench](https://github.com/akopytov/sysbench) OLTP benchmarks: `oltp_read_only.lua` and `oltp_write_only.lua`.
They use one table name `sbtest1`.
Multiple tables are not supported, but the [aurora](#aurora) benchmark is the same benchmark on 250 tables.

## xfer

Naïve money transfer (xfer) with three tables, millions of rows, and a complex read-write transaction 
{.tagline}

|Stage|Type|Description|
|-----|----|-----------|
|setup.yaml|DDL|Create schema and insert rows|
|xfer.yaml|Standard|Execute read-write transaction|

The xfer benchmark executes a complex [read-write transaction](https://github.com/square/finch/blob/main/benchmarks/xfer/trx/xfer.sql) on a nontrivial amount of data in three tables:

* `customers`: 1 million rows
* `balanaces`: 3 million rows (3 per customer)
* `xfers`: approximately 2.2 million rows (1 GB of data)

By default, the xfer stage executes a client for each CPU core; specify `-p clients=N` to burn less CPU cores.
