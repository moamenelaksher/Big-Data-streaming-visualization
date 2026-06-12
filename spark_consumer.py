from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from influxdb import InfluxDBClient


INFLUX_HOST = "localhost"
INFLUX_PORT = 8086
INFLUX_DB = "stock_database"  


print(f"Checking InfluxDB database '{INFLUX_DB}'...")
try:
    init_client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT)
    init_client.create_database(INFLUX_DB)
    init_client.close()
    print(f"InfluxDB database '{INFLUX_DB}' ready. 🎉")
except Exception as e:
    print(f"Warning: Could not connect or create database: {e}")


trade_schema = StructType([
    StructField("data", ArrayType(StructType([
        StructField("p", DoubleType(), True),      
        StructField("s", StringType(), True),     
        StructField("t", LongType(), True),       
        StructField("v", DoubleType(), True)      
    ])), True),
    StructField("type", StringType(), True)
])


spark = SparkSession.builder \
    .appName("KafkaToInfluxDB_v1.8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "earliest") \
    .load()

kafka_json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_payload")

parsed_df = kafka_json_df \
    .withColumn("parsed_json", from_json(col("json_payload"), trade_schema)) \
    .select("parsed_json.data")

flattened_df = parsed_df.withColumn("trade", explode(col("data"))) \
    .select(
        col("trade.s").alias("symbol"),
        col("trade.p").alias("price"),
        col("trade.v").alias("volume"),
        col("trade.t").alias("timestamp")
    )


def write_to_influx(batch_df, batch_id):
    """Write stock records from one micro-batch to InfluxDB 1.8."""
    from influxdb import InfluxDBClient

    rows = batch_df.collect()
    if not rows:
        return

    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, database=INFLUX_DB)
    points = []

    for row in rows:
        if row.timestamp is None:
            continue
            
        clean_symbol = str(row.symbol).replace(" ", "")
        
        points.append({
            "measurement": "crypto_prices",
            "tags": {
                "symbol": clean_symbol
            },
            "time": int(row.timestamp) * 1_000_000,  
            "fields": {
                "price": float(row.price or 0.0),
                "volume": float(row.volume or 0.0)
            }
        })

    if points:
        try:
            client.write_points(points, batch_size=500, time_precision="n")
            print(f"[agg] batch {batch_id}: wrote {len(points)} records to InfluxDB")
        except Exception as e:
            print(f"[agg] batch {batch_id}: Failed to write to InfluxDB. Error: {e}")
        finally:
            client.close()


query = flattened_df.writeStream \
    .foreachBatch(write_to_influx) \
    .start()

query.awaitTermination()
