from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.window import Window

columns = ["product", "date", "value"]


data = [
    ("p1", "d1", 10),
    ("p1", "d2", 9),
    ("p1", "d3", 20),
    ("p2", "d1", 15),
    ("p2", "d2", 30)
]
columns = ["product", "date", "value"]

# Create the DataFrame
data_df = spark.createDataFrame(data, columns)

# Show the DataFrame to verify it
data_df.show()

# Define the window specification
windowSpec = Window.partitionBy("product").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Add cumulative sum column
data_df_with_cumsum = data_df.withColumn("cumulative_sum", _sum("value").over(windowSpec))

# Show the resulting DataFrame
data_df_with_cumsum.show()


