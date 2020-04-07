import pytz
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, initcap
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, IntegerType
from pyspark.sql.functions import monotonically_increasing_id

# function that takes a datetime string from raw data and returns the localized timestamp
tz = pytz.timezone('America/Los_Angeles')
get_timestamp = udf(lambda x: tz.localize(datetime.strptime(x, '%Y/%m/%d %I:%M:%S %p')), TimestampType())


def get_spark_session():
    """
        Create or retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .appName("sf-crime") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def get_raw_crime_data(spark, path):
    """
        Get raw crime incidents data from CSV file.
        Add timestamp columns for incident and report dates.
    """

    df = spark.read.csv(path + "Police_Department_Incident_Reports__2018_to_Present.csv", header = 'true')
    df = rename_columns(df)
    df = df.filter(df["analysis_neighborhood"].isNotNull()) \
        .filter(df["incident_category"].isNotNull()) \
        .filter(df["incident_datetime"].isNotNull())
    df = df.withColumn("incident_timestamp", get_timestamp(col("incident_datetime"))) \
        .withColumn("report_timestamp", get_timestamp(col("report_datetime")))
    return df


def load_police_districts(spark, path, output_path):
    """
        Get police districts from CSV and write to parquet files.
    """

    print("-- police districts --")
    df_sfpd_districts = spark.read.csv(path + "sfpd_districts.csv", header = 'true') \
        .withColumn("district_id", monotonically_increasing_id()) \
        .select("district_id", col("COMPANY").alias("company"), initcap(col("DISTRICT")).alias("district"))
    df_sfpd_districts.show()
    df_sfpd_districts.write.mode("overwrite").parquet(output_path + "police_districts/")


def load_neighborhoods(spark, path, output_path):
    """
        Get neighborhoods from CSV and write to parquet files.
    """    

    print("-- neighborhoods --")
    df_neighborhoods = spark.read.csv(path + "San_Francisco_Analysis_Neighborhoods.csv", header = 'true') \
        .withColumn("neighborhood_id", monotonically_increasing_id())
    df_neighborhoods = df_neighborhoods.select("neighborhood_id", df_neighborhoods["NHOOD"].alias("neighborhood"))
    df_neighborhoods.show()
    df_neighborhoods.write.mode("overwrite").parquet(output_path + "neighborhoods/")


def load_categories(df, output_path):
    """
        Get crime categories from raw data and save to parquet files.
    """

    print("-- categories --")
    df_categories = df.selectExpr("incident_category as category").distinct()
    df_categories = df_categories.orderBy("category").withColumn("category_id", monotonically_increasing_id())
    df_categories = df_categories.select("category_id", "category")
    df_categories.show()
    df_categories.write.mode("overwrite").parquet(output_path + "categories/")


def load_datetime(df, output_path):
    """
        Get date and time info from unique incident and report timestamps and save to parquet files.
    """
    
    print("-- datetime --")
    df_datetime = df.select(col("incident_timestamp").alias("timestamp")).distinct()
    df_datetime = df_datetime.union(df.select(col("report_timestamp").alias("timestamp")).distinct())  
    df_datetime = df_datetime.dropDuplicates()
    df_datetime = df_datetime.withColumn("hour", hour(col("timestamp")))
    df_datetime = df_datetime.withColumn("day", dayofmonth(col("timestamp")))
    df_datetime = df_datetime.withColumn("month", month("timestamp"))
    df_datetime = df_datetime.withColumn("year", year("timestamp"))
    df_datetime = df_datetime.withColumn("week", weekofyear("timestamp"))  
    df_datetime = df_datetime.drop("datetime")
    df_datetime.show()    
    df_datetime.write.mode("overwrite").parquet(output_path + "datetime/")


def load_incidents(spark, df, output_path):
    """
        Process crime incidents data and write to parquet files.
        This represents the fact table.
    """
    
    print("-- incidents --")
    districts_table = spark.read.parquet(output_path + "police_districts")
    neighborhoods_table = spark.read.parquet(output_path + "neighborhoods")
    categories_table = spark.read.parquet(output_path + "categories")

    df_incidents = df.join(districts_table, districts_table.district == df.police_district) \
        .join(neighborhoods_table, neighborhoods_table.neighborhood == df.analysis_neighborhood) \
        .join(categories_table, categories_table.category == df.incident_category) \
        .withColumn("incident_timestamp", get_timestamp(col("incident_datetime"))) \
        .withColumn("report_timestamp", get_timestamp(col("report_datetime"))) \
        .withColumn("incident_month", month("incident_timestamp")) \
        .select(
            col("incident_id"),
            col("incident_number"),
            col("district_id"),
            col("neighborhood_id"),
            col("category_id"),
            col("incident_timestamp"),
            col("report_timestamp"),
            col("incident_description"),
            col("resolution"),
            col("intersection"),
            col("latitude"),
            col("longitude"),
            col("incident_month").alias("month"),
            col("incident_year").alias("year")
        ).repartition("year", "month")

    count = df_incidents.count()
    print("total incidents: " + str(count))
    df_incidents.show()
    df_incidents.write.mode('overwrite').partitionBy("year", "month").parquet(output_path + 'incidents/')


def rename_columns(df):
    """
        Make column headers lower case
        Replace space with underscore
    """

    for name in df.columns:
        new_name = name.lower().replace(" ", "_")
        df = df.withColumnRenamed(name, new_name)
    return df


def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")  
    path = "hdfs:///user/maria_dev/crime_data/"
    output_path = "hdfs:///user/maria_dev/output/"

    df = get_raw_crime_data(spark, path)

    load_police_districts(spark, path, output_path)
    load_neighborhoods(spark, path, output_path)
    load_categories(df, output_path)
    load_datetime(df, output_path)
    load_incidents(spark, df, output_path)

if __name__ == "__main__":
    main()    