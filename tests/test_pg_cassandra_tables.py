"""Tests for Cassandra / Clingine CQL helpers."""

import unittest

from backend import pg_cassandra as pgc


class TestPgCassandraTables(unittest.TestCase):
    def test_parse_tables_stdout_pipe_table(self) -> None:
        raw = """
 keyspace_name | table_name
---------------+------------
 foo           | bar

(1 rows)
"""
        names = pgc.parse_cassandra_tables_cqlsh_stdout(raw)
        self.assertEqual(names, ["bar"])

    def test_parse_tables_single_column(self) -> None:
        raw = """ table_name
-------------
 alpha
 beta
"""
        names = pgc.parse_cassandra_tables_cqlsh_stdout(raw)
        self.assertEqual(names, ["alpha", "beta"])

    def test_sanitize_table(self) -> None:
        self.assertEqual(pgc.sanitize_cassandra_table("raw_20260104"), "raw_20260104")
        with self.assertRaises(ValueError):
            pgc.sanitize_cassandra_table("bad;drop")


if __name__ == "__main__":
    unittest.main()
