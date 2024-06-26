from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, avg, expr

BOOTSTRAP_SERVERS = "confluent-local-broker-1:65212"
TOPIC = "wikimedia_events"

schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("user", StringType()),
    StructField("meta", StructType([
        StructField("domain", StringType())
    ])),
    StructField("length", StructType([
        StructField("old", IntegerType()),
        StructField("new", IntegerType())
    ]))
])

def main():
    spark = SparkSession.builder \
                        .appName('WikimediaStreaming') \
                        .getOrCreate()
    
    kafka_stream_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS) \
        .option('subscribe', TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    parsed_df = kafka_stream_df.select(
        from_json(col("value").cast("string"), schema).alias("data"))

    selected_cols_df = parsed_df.select(
        col("data.timestamp"),
        col("data.bot"),
        col("data.minor"),
        col("data.user"),
        col("data.meta.domain"),
        col("data.length.old").alias('old_length'),
        col("data.length.new").alias('new_length'))

    selected_cols_df = selected_cols_df.withColumn("length_diff", col("new_length") - col('old_length'))

    selected_cols_df = selected_cols_df.withColumn('length_diff_percent', expr('CASE WHEN old_length != 0 THEN (new_length - old_length)*100 / old_length ELSE NULL END'))
    
    # query = selected_cols_df.writeStream \
    #     .outputMode('append') \
    #     .format('console') \
    #     .option('truncate', False) \
    #     .start() \
    #     .awaitTermination()

    selected_cols_df.writeStream \
    .outputMode('append')\
    .option('checkpointLocation','./output/checkpoint')\
    .format('csv')\
    .option('path','./output/wikimedia_event.csv')\
    .option('header',True)\
    .trigger(processingTime='10 seconds')\
    .start() \
    .awaitTermination()

# if __name__ == "__main__":
#     main()

# Define output domains

def output_top_domains(df):
    top_domains = df.groupBy('meta.domain') \
                    .count() \
                    .orderBy('count', ascending=False) \
                    .limit(5)

    query = top_domains.writeStream \
                        .outputMode('complete') \
                        .format('console') \
                        .start()

    query.awaitTermination()

def output_top_users(df):
    top_users = df.groupBy('user') \
                  .agg(F.sum('length_diff').alias('sum_length_diff')) \
                  .orderBy('sum_length_diff', ascending=False) \
                  .limit(5)

    query = top_users.writeStream \
                     .outputMode('complete') \
                     .format('console') \
                     .start()

    query.awaitTermination()

def output_event_stats(df):
    event_stats = df.agg(
        F.count('*').alias('total_events'),
        (F.sum(F.col('bot').cast('int')) / F.count('*')).alias('percent_bot'),
        F.avg('length_diff').alias('avg_length_diff'),
        F.min('length_diff').alias('min_length_diff'),
        F.max('length_diff').alias('max_length_diff')
    )

    query = event_stats.writeStream \
                        .outputMode('complete') \
                        .format('console') \
                        .start()

    query.awaitTermination()



if __name__ == "__main__":
    main()

































































