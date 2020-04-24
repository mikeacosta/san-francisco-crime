from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_spark_session():
    """
        Create or retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .appName("sf-crime") \
        .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf()) \
        .getOrCreate()
    return spark

def get_data(spark, path):
    df1 = spark.read.orc(path + "police_districts")
    df2 = spark.read.orc(path + "neighborhoods")
    df3 = spark.read.orc(path + "categories")
    df4 = spark.read.orc(path + "date_time")
    df5 = spark.read.orc(path + "incidents")
    
    return [df1, df2, df3, df4, df5]

def main():
    path = "hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/sfcrime.db/"
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")  

    police_districts, neighborhoods, categories, date_time, incidents = get_data(spark, path) 

    df = incidents.join(police_districts, police_districts.district_id == incidents.district_id) \
        .join(neighborhoods, neighborhoods.neighborhood_id == incidents.neighborhood_id) \
        .join(categories, categories.category_id == incidents.category_id) \
        .join(date_time, date_time.ts == incidents.incident_ts) \
        .select(
            "district",
            "neighborhood",
            "category",
            col("incident_description").alias("description"),
            "intersection",
            date_time.month,
            "day",
            date_time.year
        )

    df.printSchema()   

    print("-- most common reported crime based on description --")
    df.groupBy("description").count().sort(col("count").desc()).show()

    print("-- reported car break-ins by neighborhood --")
    df.filter(col("description").like("%Theft, From Locked Vehicle%")) \
        .groupBy("neighborhood").count().sort(col("count").desc()).show()

    print("-- average reported shopliftings per month in 2019")
    df.filter((col("description").like("%Shoplifting%")) & (df["year"] == 2019)) \
        .groupBy("month").count().groupBy("month").agg({'count' : 'avg'}).sort("month").show()

    print("-- reported burglaries in each police district during 2019 --")
    df.filter((df["category"] == "Burglary") & (df["year"] == 2019)) \
        .groupBy("district").count().show()

if __name__ == "__main__":
    main()   