"""
Compute daily aggregates.

This Python script serves as a "poorperson's ETL" pipeline. It reads data from a Trino
database for a given day, computes daily aggregates, and writes the results to a
PostgreSQL database.

The script is idempotent. Run as:

    python compute-summary.py YYYY-MM-DD

"""

import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text


TRINO_ENGINE = "trino://user@localhost:8080/tpch/sf100"
POSTGRES_ENGINE = "postgresql://trino:trino@localhost:5432/trino"

SQL = """
    SELECT
      DATE(orders.orderdate) AS order_date,
      customer.mktsegment,
      nation.name AS nation_name,
      COUNT(*) AS order_count,
      SUM(orders.totalprice) AS total_price
    FROM orders
    JOIN customer ON orders.custkey = customer.custkey
    JOIN nation ON customer.nationkey = nation.nationkey
    WHERE DATE(orders.orderdate) = :target_date
    GROUP BY 1, 2, 3
"""


def parse_date(date_str: str) -> datetime:
    """
    Parse a date string in the format "YYYY-MM-DD" to a datetime object.
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        print("Error: Invalid date format. Please use YYYY-MM-DD.")
        sys.exit(1)


def main(date_str: str) -> None:
    """
    Main function to compute daily aggregates.
    """
    target_date = parse_date(date_str)

    # Create database connections
    trino_engine = create_engine(TRINO_ENGINE)
    postgres_engine = create_engine(POSTGRES_ENGINE)

    # Trino query
    trino_query = text(SQL)

    # Execute Trino query
    with trino_engine.connect() as conn:
        result = pd.read_sql(trino_query, conn, params={"target_date": target_date})

    # If no data for the given date, exit
    if result.empty:
        print(f"No data found for {date_str}")
        return

    # Upsert data into PostgreSQL
    with postgres_engine.begin() as conn:
        # First, delete existing records for the target date
        delete_query = text(
            """
        DELETE FROM daily_order_summary
        WHERE order_date = :target_date
        """
        )
        conn.execute(delete_query, {"target_date": target_date})

        # Then, insert new records
        insert_query = text(
            """
        INSERT INTO daily_order_summary 
        (order_date, mktsegment, nation_name, order_count, total_price)
        VALUES (:order_date, :mktsegment, :nation_name, :order_count, :total_price)
        """
        )
        conn.execute(insert_query, result.to_dict("records"))

    print(f"Successfully processed data for {date_str}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python compute-summary.py YYYY-MM-DD")
        sys.exit(1)

    main(sys.argv[1])
