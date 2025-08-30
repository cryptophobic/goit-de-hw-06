from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, window, to_json, struct, current_timestamp, lit, broadcast, array, explode, to_timestamp, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def main():
    # ✅ Спільні Kafka-опції (і для reader, і для writer)
    kafka_opts = {"kafka.bootstrap.servers": '77.81.230.104:9092',
                  "kafka.security.protocol": 'SASL_PLAINTEXT',
                  "kafka.sasl.mechanism": 'PLAIN',
                  "kafka.sasl.jaas.config": (
                      f"org.apache.kafka.common.security.plain.PlainLoginModule required "
                      f"username='admin' password='VawEzo1ikLtrA8Ug8THa';"
        )}


    spark = SparkSession.builder.appName("IoT-Alerts-Streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    # 1) Вхід: очікуємо JSON {id, temperature, humidity, timestamp}
    kafka_df = (
        spark.readStream.format("kafka")
        .options(**kafka_opts)
        .option("subscribe", "building_sensors_cryptophobic")
        .option("startingOffsets", "latest")
        .load()
    )

    # 1) timestamp як STRING у схемі JSON
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("timestamp", StringType(), True),  # лишаємо STRING
    ])

    # 2) парсимо у справжній TimestampType з кількома патернами ISO-8601
    parsed = (
        kafka_df
        .select(from_json(col("value").cast("string"), schema).alias("js"),
                col("timestamp").alias("kafka_ts"))
        .select(
            col("js.id").alias("id"),
            col("js.temperature").alias("temperature"),
            col("js.humidity").alias("humidity"),
            coalesce(
                # мікросекунди
                to_timestamp(col("js.timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
                # мілісекунди
                to_timestamp(col("js.timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                # без дробової частини
                to_timestamp(col("js.timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                # запасний варіант — Kafka timestamp
                col("kafka_ts")
            ).alias("event_time"),
        )
        .filter(col("event_time").isNotNull())
    )
    # (опц.) фіксуємо TZ, щоб не було зсувів
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    dbg = parsed.selectExpr(
        "CAST(event_time AS STRING) AS et", "CAST(temperature AS DOUBLE) AS t", "CAST(humidity AS DOUBLE) AS h"
    )
    dbg_q = (dbg.writeStream.format("console").outputMode("append")
             .option("truncate", "false").trigger(processingTime="5 seconds").start())

    # 2) Sliding window 1 хв, крок 30 с, watermark 10 с
    agg = (
        parsed
        .withWatermark("event_time", "10 seconds")
        .groupBy(window(col("event_time"), "1 minute", "30 seconds"))
        .agg(
            avg("temperature").alias("temperature_avg"),
            avg("humidity").alias("humidity_avg"),
        )
    )

    # 3) Конфіг алертів (додамо broadcast — менше shuffle)
    # (залишаємо як було)
    alerts_df = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv('/opt/bitnami/spark/data/alerts_conditions.csv')
        .na.fill({"temperature_min": -999, "temperature_max": -999,
                  "humidity_min": -999, "humidity_max": -999})
    )
    alerts_df = broadcast(alerts_df)

    # умови
    temp_alert_cond = (col("temperature_max") != -999) & (col("temperature_avg") > col("temperature_max")) | \
                      ((col("temperature_min") != -999) & (col("temperature_avg") < col("temperature_min")))

    hum_alert_cond = (col("humidity_max") != -999) & (col("humidity_avg") > col("humidity_max")) | \
                     ((col("humidity_min") != -999) & (col("humidity_avg") < col("humidity_min")))

    base = agg.crossJoin(alerts_df) \
        .select(
        "window", "temperature_avg", "humidity_avg", "code", "message",
        temp_alert_cond.alias("is_temp_alert"),
        hum_alert_cond.alias("is_hum_alert")
    )

    # перетворюємо в «довгий» формат: максимум 2 рядки на одне вікно (t/h)
    long_df = (
        base
        .select(
            "window", "temperature_avg", "humidity_avg", "code", "message",
            array(
                struct(lit("temperature_alerts_cryptophobic").alias("topic"),
                       col("is_temp_alert").alias("is_alert")),
                struct(lit("humidity_alerts_cryptophobic").alias("topic"),
                       col("is_hum_alert").alias("is_alert")),
            ).alias("alerts")
        )
        .select("window", "temperature_avg", "humidity_avg", "code", "message", explode("alerts").alias("a"))
        .where(col("a.is_alert"))
    )

    # Kafka-пейлоад
    out = long_df.select(
        col("a.topic").alias("topic"),
        to_json(struct(
            col("window"),
            col("temperature_avg").alias("t_avg"),
            col("humidity_avg").alias("h_avg"),
            col("code").cast("string"),
            col("message").cast("string"),
            current_timestamp().alias("timestamp"),
        )).alias("value")
    )

    # є колонка topic → НЕ використовуємо .option("topic", ...)
    query = (
        out.writeStream
        .format("kafka")
        .options(**kafka_opts)
        .option("checkpointLocation", "/opt/bitnami/spark/data/checkpoints/spark-alerts")
        .outputMode("append")
        .start()
    )
    query.awaitTermination()

if __name__ == "__main__":
    main()
