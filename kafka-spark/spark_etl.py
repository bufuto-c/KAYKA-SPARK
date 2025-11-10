# spark_etl.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, udf, to_timestamp, when, lit, current_timestamp, window
import pyspark.sql.functions as F
import datetime
import json

# Ajusta la versión de paquetes en spark-submit si es necesario
def create_spark():
    spark = SparkSession.builder \
        .appName("KafkaSensorETL") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    return spark

# Definir el schema esperado del JSON
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", StringType(), True),  # inicialmente string para parsear errores
    StructField("humidity", StringType(), True),
    StructField("vibration", StringType(), True),
    StructField("state", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("seq", IntegerType(), True),
    StructField("extra", StringType(), True)
])

def parse_timestamp(ts_str):
    # Intenta varios formatos de timestamp
    from datetime import datetime
    try:
        # ISO format
        return datetime.fromisoformat(ts_str.replace("Z","+00:00"))
    except Exception:
        try:
            # epoch seconds
            return datetime.utcfromtimestamp(int(ts_str))
        except Exception:
            return None

# UDF para normalizar timestamp
from pyspark.sql.types import TimestampType
parse_ts_udf = F.udf(lambda x: parse_timestamp(x) if x is not None else None, TimestampType())

def write_to_db(batch_df, batch_id):
    # Placeholder: aquí pondrás la lógica para insertar la batch en tu DB (jdbc, ORM, etc.)
    # Ejemplo: batch_df.write.format("jdbc")...
    print(f"[INFO] batch_id={batch_id} rows={batch_df.count()} - here you would write to a DB")

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Leer desde Kafka (bootstrap server)
    df_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor-events") \
        .option("startingOffsets", "earliest") \
        .load()

    # df_raw schema: key (binary), value (binary), topic, partition, offset, timestamp, timestampType
    # 2) Decodificar value y parsear JSON
    df_json = df_raw.selectExpr("CAST(value AS STRING) as value_str", "timestamp as kafka_recv_ts")
    df_parsed = df_json.withColumn("jsonData", from_json(col("value_str"), schema)).select("jsonData.*", "kafka_recv_ts")

    # 3) Normalizaciones y limpieza
    # - convertir campos numéricos desde strings, detectar no-números, imputar o marcar
    df_clean = df_parsed \
        .withColumn("temperature_raw", col("temperature")) \
        .withColumn("humidity_raw", col("humidity")) \
        .withColumn("vibration_raw", col("vibration")) \
        # Normalize numeric fields: try cast to double, if fails set null
        .withColumn("temperature_val", when(col("temperature").rlike("^-?\\d+(\\.\\d+)?$"),
                                           col("temperature").cast(DoubleType()))
                    .otherwise(None)) \
        .withColumn("humidity_val", when(col("humidity").rlike("^\\d+$"),
                                        col("humidity").cast(IntegerType()))
                    .otherwise(None)) \
        .withColumn("vibration_val", when(col("vibration").rlike("^-?\\d+(\\.\\d+)?$"),
                                         col("vibration").cast(DoubleType()))
                    .otherwise(None)) \
        # parse timestamp attempts (ISO or epoch)
        .withColumn("event_ts_parsed", parse_ts_udf(col("ts"))) \
        # add kafka receive time as backup
        .withColumn("recv_time", col("kafka_recv_ts")) \
        # Fill sensible defaults where appropriate
        .withColumn("state_clean", when(col("state").isNull(), lit("UNKNOWN")).otherwise(col("state")))

    # 4) Business rules / imputations:
    # If temperature_val is null but temperature_raw looks like a quoted number with comma, try replacing comma->dot
    df_clean = df_clean.withColumn("temperature_val",
                                   when(col("temperature_val").isNull() & col("temperature_raw").isNotNull() & col("temperature_raw").rlike("^\\d+,\\d+$"),
                                        col("temperature_raw").replace(",", ".").cast(DoubleType()))
                                   .otherwise(col("temperature_val")))

    # If still null, set to a sentinel or average (here we set sentinel -999.0 so can filter later)
    df_clean = df_clean.withColumn("temperature_val", when(col("temperature_val").isNull(), lit(-999.0)).otherwise(col("temperature_val")))
    df_clean = df_clean.withColumn("humidity_val", when(col("humidity_val").isNull(), lit(-1)).otherwise(col("humidity_val")))
    df_clean = df_clean.withColumn("vibration_val", when(col("vibration_val").isNull(), lit(0.0)).otherwise(col("vibration_val")))

    # 5) Flag bad records (example: bad timestamp or missing sensor_id)
    df_flagged = df_clean.withColumn("is_bad",
                                     (col("event_ts_parsed").isNull()) | (col("sensor_id").isNull()) | (col("seq").isNull()))

    # 6) Separate good / bad streams
    good = df_flagged.filter(~col("is_bad"))
    bad = df_flagged.filter(col("is_bad"))

    # 7) Example aggregations on the good stream (windowed avg temp per sensor, every 60s)
    agg = good \
        .withColumn("event_time", col("event_ts_parsed")) \
        .groupBy(window(col("event_time"), "60 seconds"), col("sensor_id")) \
        .agg(
            F.count("*").alias("count_events"),
            F.avg("temperature_val").alias("avg_temp"),
            F.avg("vibration_val").alias("avg_vibration")
        )

    # 8) Sinks: console for demo; and write out the 'good' records via foreachBatch to call write_to_db
    query_agg = agg.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .start()

    # Write good records to DB placeholder using foreachBatch
    def foreach_batch_function(df, epoch_id):
        # small transformations or validations before DB write
        # call the placeholder writer
        write_to_db(df, epoch_id)

    query_good = good.writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("append") \
        .start()

    # Also log bad records to console for auditing
    query_bad = bad.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
