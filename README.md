<div align="center">
  <a href="https://www.datainaction.dev/" title="Data In Action" target="_blank">
    <img alt="Homepage" src="https://img.shields.io/website?down_color=%23FF4136&down_message=Down&label=Homepage&logo=home-assistant&logoColor=white&up_color=%232ECC40&up_message=Up&url=https%3A%2F%2Fmegabyte.space&style=for-the-badge" />
  </a>
  <a href="https://github.com/reginaldosilva27/dtstools/blob/main/CONTRIBUTING.md" title="Contributing" target="_blank">
    <img alt="Contributing" src="https://img.shields.io/badge/Contributing-Guide-0074D9?logo=github-sponsors&logoColor=white&style=for-the-badge" />
  </a>
  <a href="https://join.slack.com/t/databricksbr/shared_invite/zt-1xe5tjy82-4O0fYQM8WnplLqrqIxQKqg" title="Slack" target="_blank">
    <img alt="Slack" src="https://img.shields.io/badge/Slack-Chat-e01e5a?logo=slack&logoColor=white&style=for-the-badge" />
  </a>
  <a href="https://github.com/reginaldosilva27/dtstools" title="GitHub" target="_blank">
    <img alt="GitHub" src="https://img.shields.io/badge/Mirror-GitHub-333333?logo=github&style=for-the-badge" />
  </a>
</div>
<div align="center">
  <a title="Version: 0.0.3" href="https://github.com/reginaldosilva27/dtstools" target="_blank">
    <img alt="Version: 0.0.3" src="https://img.shields.io/badge/version-0.0.3-blue.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgAQMAAABJtOi3AAAABlBMVEUAAAD///+l2Z/dAAAAAXRSTlMAQObYZgAAACNJREFUCNdjIACY//+BEp9hhM3hAzYQwoBIAqEDYQrCZLwAAGlFKxU1nF9cAAAAAElFTkSuQmCC&cacheSeconds=2592000&style=flat-square" />
  </a>
    <a title="License: MIT" href="https://github.com/reginaldosilva27/dtstools" target="_blank">
    <img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-yellow.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgAQMAAABJtOi3AAAABlBMVEUAAAD///+l2Z/dAAAAAXRSTlMAQObYZgAAAHpJREFUCNdjYOD/wMDAUP+PgYHxhzwDA/MB5gMM7AwMDxj4GBgKGGQYGCyAEEgbMDDwAAWAwmk8958xpIOI5zKH2RmOyhxmZjguAiKmgIgtQOIYmFgCIp4AlaQ9OczGkJYCJEAGgI0CGwo2HmwR2Eqw5SBnNIAdBHYaAJb6KLM15W/CAAAAAElFTkSuQmCC&style=flat-square" />
  </a>
    <a href="Spark" title="Platform" target="_blank">
    <img alt="GitLab" src="https://img.shields.io/badge/platform-spark-red" />
  </a>
</div>

<a href="#introducion" style="width:100%"><img style="width:100%" src="https://static.wixstatic.com/media/a794bc_b10266580b524f6586d3b2d835cfb036~mv2.png" /></a>

<div align="center">
  <h1 align="center">DTSTOOLS: Help with your Delta Lake <i></i></h1>
  <h4 style="color: #18c3d1;">Library created by <a href="https://www.datainaction.dev/" target="_blank">Data In Action</a></h4><i></i>
</div>

```
#####    ######    #####   ######     ###      ###    ####      #####   
 ## ##     ##     ##   ##    ##      ## ##    ## ##    ##      ##   ##  
 ##  ##    ##     ##         ##     ##   ##  ##   ##   ##      ##       
 ##  ##    ##      #####     ##     ##   ##  ##   ##   ##       #####   
 ##  ##    ##          ##    ##     ##   ##  ##   ##   ##           ##  
 ## ##     ##     ##   ##    ##      ## ##    ## ##    ##      ##   ##  
#####      ##      #####     ##       ###      ###    #######   #####   
 ```

<a href="#table-of-content" style="width:100%"><img style="width:100%" src="https://static.wixstatic.com/media/a794bc_b10266580b524f6586d3b2d835cfb036~mv2.png" /></a>

## Table of Contents

- [Introduction](#introducion)
- [How to use dtstools](#how-to-use-dtstools)
- [How to use tableSize](#how-to-use-tableSize)
- [How to use tableMaintenance](#how-to-use-tableMaintenance)
- [Future implementations](#future-implementations)
- [Notes](#notes)
- [References](#references)


<a href="#introducion" style="width:100%"><img style="width:100%" src="https://static.wixstatic.com/media/a794bc_b10266580b524f6586d3b2d835cfb036~mv2.png" /></a>

## Introduction

| version | date | description |
|-----------|-------|----------|
| `v0.0.3` | 2023-06-17 | Basic features |

> This package aims to provide functionality to work with Delta Lake.
>
> Facilitating the visualization of the actual size (Storage) of your Delta tables and their maintenance (Vacuum and Optimize)

> Below are the steps performed in order of execution:
1. Executed a **describe detail** to get the current location and size
2. A scan (dbutils.fs.ls) is performed on the Storage folders recursively to calculate the space used in the Storage, excluding the _delta_logs, with this we can calculate an average of how much can be released with Vacuum
3. Returns a Dataframet

<a href="#dtstools" style="width:100%"><img style="width:100%" src="https://static.wixstatic.com/media/a794bc_b10266580b524f6586d3b2d835cfb036~mv2.png" /></a>

<a id="how-to-use-dtstools"></a>
### How to use dtstools
**First install the package via PyPi**

```
pip install --upgrade -i https://test.pypi.org/simple/ dtstools
```

**Import the dtsTable module into your context**

```
from dtstools import dtsTable
```

**Use the function dtsTable.Help() to see examples of function usage and a summary of each function.**

```
dtsTable.Help()
```
<a href="#tablesize" style="width:100%"><img style="width:100%" src="https://static.wixstatic.com/media/a794bc_b10266580b524f6586d3b2d835cfb036~mv2.png" /></a>

<a id="how-to-use-tableSize"></a>
### How to use tableSize
> Find out the true size of your table
Call the tableSize function passing the database and table name, use display() to see the results.

**This function returns a Dataframe.**

```
dtsTable.tableSize(databaseName,tableName).display()
```

**Save the result in a Delta Table for monitoring and baseline**

```
dtsTable.tableSize(databaseName,tableName) \
 .write.format('delta') \
 .mode('append') \
 .saveAsTable("db_demo.tableSize",path='abfss://container@storage.dfs.core.windows.net/bronze/tableSize')
 ```

**Get the size of all tables in your database**
```
for tb in spark.sql(f"show tables from db_demo").collect():
    try:
        print(">> Collecting data... Table:",tb.tableName)
        dtsTable.tableSize(tb.database,tb.tableName) \
        .write.format('delta') \
        .mode('append') \
        .saveAsTable("db_demo.tableSize",path='abfss://container@storage.dfs.core.windows.net/bronze/tableSize')
    except Exception as e:
        print (f"###### Error to load tableName {tb.tableName} - {e}######")
```

<a href="#tableMaintenance" style="width:100%"><img style="width:100%" src="https://static.wixstatic.com/media/a794bc_b10266580b524f6586d3b2d835cfb036~mv2.png" /></a>

<a id="how-to-use-tableMaintenance"></a>
### How to use tableMaintenance
Apply maintenance to a table, Optimize and Vacuum.

| Parameter | Description | Type
| ------------- | ------------- | ------------- |
| schemaName | Name of the database where the table is created | string |
| tableName | Name of the table that will be applied in the maintenance | string |
| vacuum | True: Vacuum will run, False: Ignore vacuum | bool |
| optimize | True: OPTIMIZE will be executed, False: Skip OPTIMIZE | bool |
| zorderColumns | If informed and optimize is equal to True, Zorder is applied to the list of columns separated by a comma (,) | string |
| vacuumRetention | Number of hours to hold after vacuum runs | whole |
| Debug | Just print the result on screen | bool |

**Apply in a single table.**
```
dtsTable.tableMaintenance(schemaName="Database", tableName="tableName", zorderColumns='none', vacuumRetention=168, vacuum=True, optimize=True, debug=False)
```

**Apply maintenance to all tables for YOUR database**
```
for tb in spark.sql(f"show tables from db_demo").collect():
  dtsTable.tableMaintenance(schemaName=tb.database, tableName=tb.tableName, zorderColumns='none', vacuumRetention=168, vacuum=True, optimize=True, debug=False)
```

<a href="#future-implementations" style="width:100%"><img style="width:100%" src="https://static.wixstatic.com/media/a794bc_b10266580b524f6586d3b2d835cfb036~mv2.png" /></a>

<a id="future-implementations"></a>
## Future implementations

> 1. Use Unity Catalog<br>
> 2. Run for all databases<br>
> 3. Minimize costs with dbutils.fs.ls by looking directly into the transaction log<br>

<a href="#notes" style="width:100%"><img style="width:100%" src="https://static.wixstatic.com/media/a794bc_b10266580b524f6586d3b2d835cfb036~mv2.png" /></a>

<a id="notes"></a>
## Notes
> - For partitioned tables with many partitions, the execution time can take longer, so monitor the first executions well, the use is at your responsibility, despite not having any risk mapped so far, it can only generate more transactions for your storage
> - Cost of Azure Storage Transactions: Read Operations (per 10,000) - $0.0258 (two cents per 10,000 operations) (Estimated price as of 4/21/2023)

<a href="#references" style="width:100%"><img style="width:100%" src="https://static.wixstatic.com/media/a794bc_b10266580b524f6586d3b2d835cfb036~mv2.png" /></a>

<a id="references"></a>
## References

https://github.com/reginaldosilva27/dtstools
<br>

> ``Author: Reginaldo Silva``
  - [Blog Data In Action](https://datainaction.dev/)
  - [Github](https://github.com/reginaldosilva27/dtstools)
  - [Dataside](https://www.dataside.com.br/)