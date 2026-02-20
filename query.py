import duckdb
from pathlib import Path

# Get latest parquet file
PARQUET_FILE = sorted(Path("data/processed").glob("flights_clean_*.parquet"))[-1]

con = duckdb.connect()

# Basic queries
print("\nTotal rows:")
print(con.execute(f"""
    SELECT COUNT(*) 
    FROM read_parquet('{PARQUET_FILE}')
""").fetchall())

print("\nTop 5 countries by aircraft count:")
print(con.execute(f"""
    SELECT origin_country, COUNT(*) AS aircraft_count
    FROM read_parquet('{PARQUET_FILE}')
    GROUP BY origin_country
    ORDER BY aircraft_count DESC
    LIMIT 5
""").fetchall())

print("\nAverage altitude by country (top 5):")
print(con.execute(f"""
    SELECT origin_country, ROUND(AVG(altitude), 2) AS avg_altitude
    FROM read_parquet('{PARQUET_FILE}')
    GROUP BY origin_country
    ORDER BY avg_altitude DESC
    LIMIT 5
""").fetchall())
