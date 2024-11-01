from db import jdbc_url, connection_properties
from schema import adsb_schema, oag_schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum, explode, to_date, to_timestamp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType

# Initialize Spark Session
# partitions can be adjusted accordingly
spark = SparkSession.builder \
    .appName("FlightDataProcessing") \
    .master("spark://spark:7077") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

def process():
    # Load data from JSON files
    adsb_df = spark.read \
        .option("multiline", "true") \
        .json("/app/data/adsb_multi_aircraft.json", schema=adsb_schema)
    oag_df = spark.read \
        .option("multiline", "true") \
        .json("/app/data/oag_multiple.json", schema=oag_schema) \
        .selectExpr("explode(data) as data") \
        .select("data.*")
    
    # Explode statusDetails array to access nested delay details
    exploded_df = oag_df.withColumn("statusDetails", F.explode("statusDetails"))
    # Cache exploded DataFrame if used multiple times
    exploded_df.cache()

    process_delay(exploded_df)
    process_general_data(oag_df, adsb_df)

    print("Processing complete. Results saved to the PostgreSQL database.")


def process_delay(exploded_df):
    # Filter for delayed departures and arrivals
    delayed_departures = exploded_df.filter(
        F.col("statusDetails.departure.actualTime.outGateTimeliness") == "Delayed"
    ).select(
        F.col("departure.date.utc").alias("departure_date")
    )

    delayed_arrivals = exploded_df.filter(
        F.col("statusDetails.arrival.actualTime.inGateTimeliness") == "Delayed"
    ).select(
        F.col("departure.date.utc").alias("departure_date")  # Assuming departure_date is same as departure date for arrivals
    )

    # Aggregate delay counts by departure_date
    departure_delay_counts = delayed_departures.groupBy("departure_date") \
        .agg(F.count("*").alias("departure_delay_count"))

    arrival_delay_counts = delayed_arrivals.groupBy("departure_date") \
        .agg(F.count("*").alias("arrival_delay_count"))

    # Join delay counts on departure_date to combine results
    delays_df = departure_delay_counts.join(arrival_delay_counts, "departure_date", "outer") \
        .fillna(0)  # Fill any missing delay counts with 0

    # Display the result
    delays_df.show()

    # write to DB
    delays_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "delayed_flights") \
        .options(**connection_properties) \
        .partitionBy("departure_date") \
        .mode("append") \
        .save()


def process_general_data(oag_df, adsb_df):
    # Insert into airports table with optional fields handling
    airports_df = oag_df.selectExpr(
        "departure.airport.iata AS iata_code",
        "departure.airport.icao AS icao_code",
        "departure.airport.faa AS faa_code"
    ).distinct()
    
    airports_df.write.jdbc(url=jdbc_url, table="airports", mode="append", properties=connection_properties)

    # Insert into airlines table with optional fields handling
    airlines_df = oag_df.selectExpr(
        "carrier.iata AS iata_code",
        "carrier.icao AS icao_code"
    ).distinct()

    airlines_df.write.jdbc(url=jdbc_url, table="airlines", mode="append", properties=connection_properties)

    # Insert into flights table with optional fields handling
    flights_df = oag_df.select(
        col("carrier.iata").alias("carrier_iata"),
        col("flightNumber").alias("flight_number"),
        col("departure.airport.iata").alias("departure_airport_iata"),
        to_date(col("departure.date.local")).alias("departure_date_local"),
        to_timestamp(col("departure.time.local"), "HH:mm").alias("departure_time_local"),
        col("arrival.airport.iata").alias("arrival_airport_iata"),
        to_date(col("arrival.date.local")).alias("arrival_date_local"),
        to_timestamp(col("arrival.time.local"), "HH:mm").alias("arrival_time_local")
    )
    flights_df.write.jdbc(url=jdbc_url, table="flights", mode="append", properties=connection_properties)
