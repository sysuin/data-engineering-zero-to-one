# compile-only: PySpark is not installed; this is the same job as 03_map_reduce.py
# Revenue per account in Spark: read in slices, group, write. Spark
# plans the map, the shuffle and the reduce that 03 wrote by hand.
import sqlite3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from groundwork.config import TRADELINE_DB

spark = (SparkSession.builder
         .appName("revenue-per-account")
         .master("local[4]")                   # four workers, one host
         .config("spark.sql.shuffle.partitions", "4")   # four reducers
         .getOrCreate())

# Spark reads a database in slices of a numeric column, one slice per
# task: the same ranges of order id the mappers read in 03.
con = sqlite3.connect(f"file:{TRADELINE_DB}?mode=ro", uri=True)
(top,) = con.execute("SELECT max(id) FROM orders").fetchone()
url = f"jdbc:sqlite:{TRADELINE_DB}"      # needs the SQLite JDBC driver


def table(name, column):
    return spark.read.jdbc(url, name, column=column, lowerBound=0,
                           upperBound=top, numPartitions=4)


orders = table("orders", "id").where(F.col("status") == "placed")
lines = table("order_lines", "order_id")

revenue = (lines.join(orders, lines.order_id == orders.id)
                .groupBy("account_id")     # the shuffle happens here
                .agg(F.sum(F.col("qty") * F.col("unit_price_cents"))
                      .alias("cents"),
                     F.count("*").alias("lines")))

revenue.explain()        # look for "Exchange hashpartitioning"
rows = revenue.orderBy("account_id").collect()   # the action: it runs
total = sum(r["cents"] for r in rows)
print(f"accounts: {len(rows):,}; cents: {total:,}")
spark.stop()
