
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

if __name__ == "__main__":
    spark = SparkSession.builder.config("spark.jars", "postgresql-42.2.5.jar").master("local").appName("PostgresReaderSpark").getOrCreate()

    limit = 100
    offset = 100
    query = f"select * from acqdat_sensors_data limit {limit}"

    df = (spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://188.166.230.56:5431/acqdat_core_dev")
        .option("driver", "org.postgresql.Driver")
        .option("query", query)
        .option("user", "postgres")
        .option("password", "postgres")
        .option("numPartitions", 2)
        .load())

    
    df.printSchema()

    df.show(10)
