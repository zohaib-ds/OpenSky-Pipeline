import duckdb
from pathlib import Path

latest = sorted(Path("data/processed").rglob("flights.parquet"))[-1]

con = duckdb.connect()

print("\nFlights in snapshot:")
print(con.execute(f"""
    SELECT COUNT(*) FROM read_parquet('{latest}')
""").fetchone())

print("\nTop 5 countries by flights:")
print(con.execute(f"""
    SELECT origin_country, COUNT(*) AS flights
    FROM read_parquet('{latest}')
    GROUP BY origin_country
    ORDER BY flights DESC
    LIMIT 5
""").fetchall())

print("\nAverage altitude & speed:")
print(con.execute(f"""
    SELECT 
        ROUND(AVG(altitude), 2) AS avg_altitude,
        ROUND(AVG(velocity), 2) AS avg_speed
    FROM read_parquet('{latest}')
""").fetchone())
