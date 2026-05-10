from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import happybase

analyzer = SentimentIntensityAnalyzer()

# ---------------- SENTIMENT FUNCTION ----------------
def get_sentiment(text):
    score = analyzer.polarity_scores(text)['compound']
    if score >= 0.05:
        return "Positive"
    elif score <= -0.05:
        return "Negative"
    else:
        return "Neutral"

sentiment_udf = udf(get_sentiment, StringType())

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder \
    .appName("KafkaSparkHBase") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------- KAFKA STREAM ----------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test") \
    .option("startingOffsets", "latest") \
    .load()

# ---------------- PARSE JSON ----------------
df = df.selectExpr("CAST(value AS STRING) as json")

df = df.select(
    get_json_object(col("json"), "$.text").alias("text")
)

df = df.filter(col("text").isNotNull())

# ---------------- ADD SENTIMENT ----------------
df = df.withColumn("sentiment", sentiment_udf(col("text")))

# ---------------- DEBUG OUTPUT ----------------
query_console = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

# ---------------- HBASE WRITER ----------------
def write_to_hbase(batch_df, batch_id):
    print(f"\n🔥 Writing batch {batch_id} to HBase...")

    connection = happybase.Connection('hbase', port=9090)
    table = connection.table('sentiment_table')

    for row in batch_df.collect():
        text = row.text
        sentiment = row.sentiment

        row_key = f"{text}_{batch_id}"

        print("Writing:", row_key, sentiment)

        table.put(
            row_key.encode(),
            {
                b'data:text': text.encode(),
                b'data:sentiment': sentiment.encode()
            }
        )

    connection.close()

# ---------------- STREAM TO HBASE ----------------
query_hbase = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_hbase) \
    .start()

query_console.awaitTermination()
query_hbase.awaitTermination()