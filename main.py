from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    df = spark.range(10000)

    df.write.csv("/tmp/range", header=True)
