# Exasol Data Lineage

An Exasol script written on Lua that allows to perform Data Lineage analysis.

## How it works?

The script analyzes SQL without running it by means of in-built SQL parsing library. For each output column it identifies a list of source columns.  

## Features

* determines output columns origin
* multiple schemas
* multiple source columns

### Supported SQL constructions

* CTE
* UNION
* FROM
* JOINS
* EMITS
* Subqueries
* LOCAL keyword
* quoted identifiers
* expression columns
* table and column aliases

## Limitations

* the script doesn't check SQL syntax
* only one statement at a time
* ON and USING clauses, WHERE conditions are not analyzed yet

## Installation

1. Connect to Exasol cluster
2. Open schema in which you want to install scripts
3. Execute *.sql files from scripts directory  

## How to use?

SQL_DATA_LINEAGE script expects 2 arguments:
1. SQL statement. It is allowed to pass SELECT or CREATE VIEW statements.
2. Current schema. If null value passed, script takes current schema from session.

## Examples
 
```sql
EXECUTE SCRIPT FN.SQL_DATA_LINEAGE(
    'CREATE OR REPLACE VIEW test_view AS SELECT * FROM users',
    'TEST_DATA_LINEAGE'
)
```

Output

```text
+-------------+--------------------+--------------------+--------------------+----------+--------+------------------+
| COLUMN_NAME | SOURCE_SCHEMA_NAME | SOURCE_OBJECT_NAME | SOURCE_COLUMN_NAME | FNAME    | IS_AGG | ORDINAL_POSITION |
+-------------+--------------------+--------------------+--------------------+----------+--------+------------------+
| USER_ID     | TEST_DATA_LINEAGE  | USERS              | USER_ID            | (null)   | false  | 1                |
| NAME        | TEST_DATA_LINEAGE  | USERS              | NAME               | (null)   | false  | 2                |
| REGISTERED  | TEST_DATA_LINEAGE  | USERS              | REGISTERED         | (null)   | false  | 3                |
| STATUS      | TEST_DATA_LINEAGE  | USERS              | STATUS             | (null)   | false  | 4                |
+-------------+--------------------+--------------------+--------------------+----------+--------+------------------+
```

```sql
EXECUTE SCRIPT FN.SQL_DATA_LINEAGE(
    '
    WITH
        users AS (
            SELECT
                  user_id
                , name
                , status AS status_id
            FROM users
            WHERE status != 3
        ),

        status AS (
            SELECT
                  id AS status_id
                , name AS status_name
            FROM dim_status
        )

    SELECT
          a.*
        , COALESCE(b.status_name, ''Unknown'') AS status_name
    FROM users a
    LEFT JOIN status b ON (a.status_id = b.status_id)
    ',
    'TEST_DATA_LINEAGE'
)
```

Output

```text
+-------------+--------------------+--------------------+--------------------+----------+--------+------------------+
| COLUMN_NAME | SOURCE_SCHEMA_NAME | SOURCE_OBJECT_NAME | SOURCE_COLUMN_NAME | FNAME    | IS_AGG | ORDINAL_POSITION |
+-------------+--------------------+--------------------+--------------------+----------+--------+------------------+
| USER_ID     | TEST_DATA_LINEAGE  | USERS              | USER_ID            | (null)   | false  | 1                |
| NAME        | TEST_DATA_LINEAGE  | USERS              | NAME               | (null)   | false  | 2                |
| STATUS_ID   | TEST_DATA_LINEAGE  | USERS              | STATUS             | (null)   | false  | 3                |
| STATUS_NAME | TEST_DATA_LINEAGE  | DIM_STATUS         | NAME               | COALESCE | false  | 4                |
+-------------+--------------------+--------------------+--------------------+----------+--------+------------------+
```

## Running tests

* install [PyEXASOL](https://github.com/badoo/pyexasol) driver
* set Exasol credentials in tests/config.py

```shell script
cd tests/
python -m unittest test_sql_data_lineage.py
```

## Authors

* Dmitry Umarov <d.umarov@magiclab.co>