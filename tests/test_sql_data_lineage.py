"""
Tests for sql_data_lineage script
"""

import pyexasol
import config
import unittest

def setup(conn):
    conn.execute("CREATE SCHEMA IF NOT EXISTS {schema!i}", {'schema': config.schema})
    conn.open_schema(config.schema)

    conn.execute("""
        CREATE OR REPLACE TABLE users
        (
            user_id         DECIMAL(18,0),
            name            VARCHAR(255),
            registered      TIMESTAMP,
            status          DECIMAL(2,0)
        )
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE dim_status
        (
            id              DECIMAL(3,0),
            name            VARCHAR(255)
        )
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE dim_brand
        (
            id              DECIMAL(3,0),
            name            VARCHAR(255)
        )
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE f_billing
        (
            transaction_id  DECIMAL(18,0),
            user_id         DECIMAL(18,0),
            amount          DECIMAL(18,2),
            ts              TIMESTAMP,
            status          DECIMAL(2,0)
        )
    """)

    conn.commit()

def cleanup(conn):
    conn.execute("DROP TABLE users")
    conn.execute("DROP TABLE dim_status")
    conn.execute("DROP TABLE dim_brand")
    conn.execute("DROP TABLE f_billing")
    conn.commit()

def exec(sql, conn):
    sql = "EXECUTE SCRIPT fn.sql_data_lineage('%s', '%s')" % (sql.replace("'", "''"), config.schema)
    stmt = conn.execute(sql)
    out = []
    for row in stmt.fetchall():
        out.append((
            row['column_name'],
            row['source_schema_name'],
            row['source_object_name'],
            row['source_column_name'],
            row['ordinal_position']
        ))
    return out

class SQLDataLineageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, autocommit=False, fetch_dict=True, lower_ident=True)
        setup(cls.conn)
        cls.schema_name = config.schema.upper()

    @classmethod
    def tearDownClass(cls):
        cleanup(cls.conn)
        cls.conn.close()

    def test_simple(self):
        rows = exec('SELECT * FROM users', self.conn)
        self.assertEqual(rows, [
            ('USER_ID', self.schema_name, 'USERS', 'USER_ID', 1),
            ('NAME', self.schema_name, 'USERS', 'NAME', 2),
            ('REGISTERED', self.schema_name, 'USERS', 'REGISTERED', 3),
            ('STATUS', self.schema_name, 'USERS', 'STATUS', 4)
        ])

    def test_schema_check(self):
        rows = exec('SELECT * FROM %s.users' % (config.schema), self.conn)
        self.assertEqual(rows, [
            ('USER_ID', self.schema_name, 'USERS', 'USER_ID', 1),
            ('NAME', self.schema_name, 'USERS', 'NAME', 2),
            ('REGISTERED', self.schema_name, 'USERS', 'REGISTERED', 3),
            ('STATUS', self.schema_name, 'USERS', 'STATUS', 4)
        ])

    def test_simple_view(self):
        rows = exec('CREATE OR REPLACE VIEW test_view AS SELECT * FROM users', self.conn)
        self.assertEqual(rows, [
            ('USER_ID', self.schema_name, 'USERS', 'USER_ID', 1),
            ('NAME', self.schema_name, 'USERS', 'NAME', 2),
            ('REGISTERED', self.schema_name, 'USERS', 'REGISTERED', 3),
            ('STATUS', self.schema_name, 'USERS', 'STATUS', 4)
        ])

    def test_simple_view_with_brackets(self):
        rows = exec('CREATE OR REPLACE VIEW test_view AS (SELECT * FROM users)', self.conn)
        self.assertEqual(rows, [
            ('USER_ID', self.schema_name, 'USERS', 'USER_ID', 1),
            ('NAME', self.schema_name, 'USERS', 'NAME', 2),
            ('REGISTERED', self.schema_name, 'USERS', 'REGISTERED', 3),
            ('STATUS', self.schema_name, 'USERS', 'STATUS', 4)
        ])

    def test_select_from_subquery(self):
        rows = exec('SELECT a.user_id, a.name FROM (SELECT * FROM users) a', self.conn)
        self.assertEqual(rows, [
            ('USER_ID', self.schema_name, 'USERS', 'USER_ID', 1),
            ('NAME', self.schema_name, 'USERS', 'NAME', 2)
        ])

    def test_select_from_cte(self):
        rows = exec("""
            WITH
                users_summary AS (
                    SELECT
                          CAST(registered AS DATE) AS dt
                        , COUNT(*) AS total
                    FROM %s.users
                    GROUP BY 1
                )
        
            SELECT
                  dt
                , total
            FROM users_summary
        """ % (config.schema), self.conn)
        self.assertEqual(rows, [
            ('DT', self.schema_name, 'USERS', 'REGISTERED', 1),
            ('TOTAL', None, None, None, 2)
        ])

    def test_select_from_multiple_cte(self):
        rows = exec("""
            WITH
                users AS (
                    SELECT
                          user_id
                        , name
                        , status AS status_id
                    FROM %s.users
                    WHERE status != 3
                ),
    
                status AS (
                    SELECT
                          id AS status_id
                        , name AS status_name
                    FROM %s.dim_status
                )

            SELECT
                  a.*
                , b.status_name
            FROM users a
            JOIN status b ON (a.status_id = b.status_id)
        """ % (config.schema, config.schema), self.conn)
        self.assertEqual(rows, [
            ('USER_ID', self.schema_name, 'USERS', 'USER_ID', 1),
            ('NAME', self.schema_name, 'USERS', 'NAME', 2),
            ('STATUS_ID', self.schema_name, 'USERS', 'STATUS', 3),
            ('STATUS_NAME', self.schema_name, 'DIM_STATUS', 'NAME', 4)
        ])

    def test_expression_with_aggregated_column(self):
        rows = exec("""
            WITH
                users AS (
                    SELECT
                          user_id
                        , name
                    FROM %s.users
                    WHERE status != 3
                ),

                paid_users AS (
                    SELECT
                          user_id
                        , SUM(amount) AS total_amount
                    FROM %s.f_billing
                    WHERE status = 1
                    GROUP BY 1
                )

            SELECT
                a.*,
                b.total_amount
            FROM users a
            JOIN paid_users b ON (a.user_id = b.user_id)
        """ % (config.schema, config.schema), self.conn)
        self.assertEqual(rows, [
            ('USER_ID', self.schema_name, 'USERS', 'USER_ID', 1),
            ('NAME', self.schema_name, 'USERS', 'NAME', 2),
            ('TOTAL_AMOUNT', self.schema_name, 'F_BILLING', 'AMOUNT', 3)
        ])

    def test_multicolumn_expression(self):
        rows = exec("""
            WITH
                users AS (
                    SELECT
                          user_id
                        , name
                    FROM %s.users
                    WHERE status != 3
                ),

                paid_users AS (
                    SELECT
                          user_id
                        , SUM(amount) AS total_amount
                    FROM %s.f_billing
                    WHERE status = 1
                    GROUP BY 1
                )

                SELECT
                    a.*,
                    b.total_amount,
                    COUNT(DISTINCT COALESCE(a.user_id, b.user_id, 0)) AS total
                FROM users a
                JOIN paid_users b ON (a.user_id = b.user_id)
                GROUP BY 1,2,3
        """ % (config.schema, config.schema), self.conn)
        self.assertEqual(rows, [
            ('USER_ID', self.schema_name, 'USERS', 'USER_ID', 1),
            ('NAME', self.schema_name, 'USERS', 'NAME', 2),
            ('TOTAL_AMOUNT', self.schema_name, 'F_BILLING', 'AMOUNT', 3),
            ('TOTAL', self.schema_name, 'USERS', 'USER_ID', 4),
            ('TOTAL', self.schema_name, 'F_BILLING', 'USER_ID', 4)
        ])

    def test_cte_with_union(self):
        rows = exec("""
            -- lack of logic, but it's correct
            WITH
                a AS (
                    SELECT *
                    FROM %s.dim_status
                )
                
                SELECT
                    id,
                    description AS descr,
                    COUNT(local.descr) AS description
                FROM (
                    SELECT id, name AS description
                    FROM %s.dim_brand
                    
                    UNION ALL 
                    
                    SELECT *
                    FROM a
                    
                    UNION
                    (   
                        SELECT
                              user_id
                            , name
                        FROM %s.users
                    )
                
                )
                GROUP BY 1,2
        """ % (config.schema, config.schema, config.schema), self.conn)
        self.assertEqual(rows, [
            ('ID', self.schema_name, 'DIM_BRAND', 'ID', 1),
            ('ID', self.schema_name, 'DIM_STATUS', 'ID', 1),
            ('ID', self.schema_name, 'USERS', 'USER_ID', 1),
            ('DESCR', self.schema_name, 'DIM_BRAND', 'NAME', 2),
            ('DESCR', self.schema_name, 'DIM_STATUS', 'NAME', 2),
            ('DESCR', self.schema_name, 'USERS', 'NAME', 2),
            ('DESCRIPTION', self.schema_name, 'DIM_BRAND', 'NAME', 3),
            ('DESCRIPTION', self.schema_name, 'DIM_STATUS', 'NAME', 3),
            ('DESCRIPTION', self.schema_name, 'USERS', 'NAME', 3)
        ])

    def test_local_keyword_and_double_quotes(self):
        rows = exec("""
            SELECT
                  registered
                , COUNT(user_id) AS "count"
                , local."count" AS count_alias
            FROM %s.users
            GROUP BY 1
        """ % (config.schema), self.conn)
        self.assertEqual(rows, [
            ('REGISTERED', self.schema_name, 'USERS', 'REGISTERED', 1),
            ('COUNT', self.schema_name, 'USERS', 'USER_ID', 2),
            ('COUNT_ALIAS', self.schema_name, 'USERS', 'USER_ID', 3)
        ])

    def test_columns_without_alias_in_expression(self):
        rows = exec('SELECT COUNT(user_id), COUNT(1), SUM(1) FROM users', self.conn)
        self.assertEqual(rows, [
            ('COUNT(USER_ID)', self.schema_name, 'USERS', 'USER_ID', 1),
            ('COUNT(1)', None, None, None, 2),
            ('SUM(1)', None, None, None, 3)
        ])

    def test_emitting_func(self):
        rows = exec("""
            SELECT
                  user_id
                , %s.fake_emitting_func(
                      s.id
                    , u.registered
                )
                EMITS (
                      metric_name VARCHAR(100)
                    , metric_val DECIMAL(18,0)
                )
                FROM %s.users u
                JOIN %s.dim_status s ON (u.status = s.id)
        """ % (config.schema, config.schema, config.schema), self.conn)
        self.assertEqual(rows, [
             ('USER_ID', self.schema_name, 'USERS', 'USER_ID', 1),
             ('METRIC_NAME', self.schema_name, 'DIM_STATUS', 'ID', 2),
             ('METRIC_NAME', self.schema_name, 'USERS', 'REGISTERED', 2),
             ('METRIC_VAL', self.schema_name, 'DIM_STATUS', 'ID', 3),
             ('METRIC_VAL', self.schema_name, 'USERS', 'REGISTERED', 3)
        ])

    def test_missing_aliases_in_from_and_join(self):
        rows = exec("""
            WITH
                users AS (
                    SELECT
                          user_id
                        , user_name
                        , status_name
                    FROM (
                        SELECT
                              user_id
                            , name AS user_name
                            , status AS status_id
                        FROM %s.users
                    )
                    JOIN (
                        SELECT
                              id AS status_id
                            , name AS status_name
                        FROM %s.dim_status
                    ) USING (status_id)
                )

            SELECT *
            FROM users
        """ % (config.schema, config.schema), self.conn)
        self.assertEqual(rows, [
            ('USER_ID', self.schema_name, 'USERS', 'USER_ID', 1),
            ('USER_NAME', self.schema_name, 'USERS', 'NAME', 2),
            ('STATUS_NAME', self.schema_name, 'DIM_STATUS', 'NAME', 3)
        ])
