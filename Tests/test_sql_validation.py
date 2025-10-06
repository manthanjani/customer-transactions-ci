import sqlite3
import pytest
import glob

@pytest.fixture(scope="module")
def db():
    conn = sqlite3.connect("fraud_test.db")
    yield conn
    conn.close()

def test_load_data(db):
    # Load CSVs into the database
    import csv
    import os

    data_files = {
        "customers": "data/customers.csv",
        "transactions": "data/transactions.csv"
    }

    for table, path in data_files.items():
        assert os.path.exists(path), f"{path} not found!"
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            placeholders = ",".join(["?"] * len(headers))
            db.execute(f"DROP TABLE IF EXISTS {table}")
            db.execute(f"CREATE TABLE {table} ({','.join(headers)})")
            db.executemany(
                f"INSERT INTO {table} VALUES ({placeholders})",
                [row for row in reader]
            )
    db.commit()

def test_sql_scripts_run(db):
    # Validate syntax and execution of all SQL scripts
    sql_files = glob.glob("sql/*.sql")
    for sql_file in sql_files:
        with open(sql_file, "r", encoding="utf-8") as f:
            script = f.read()
        try:
            db.executescript(script)
        except Exception as e:
            pytest.fail(f"SQL execution failed for {sql_file}: {e}")

def test_no_negative_amounts(db):
    cursor = db.execute("SELECT COUNT(*) FROM transactions WHERE amount < 0;")
    count = cursor.fetchone()[0]
    assert count == 0, f"{count} transactions have negative amounts!"
