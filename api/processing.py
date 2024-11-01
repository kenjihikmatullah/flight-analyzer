from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FlightDataProcessing") \
    .master("spark://spark:7077") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
    .getOrCreate()

# Define schemas for both JSON files
adsb_schema = StructType([
    StructField("AircraftId", StringType(), True),
    StructField("Origin", StringType(), True),
    StructField("Destination", StringType(), True),
    StructField("Flight", StringType(), True),
    StructField("Onground", IntegerType(), True),
    StructField("Callsign", StringType(), True),
    StructField("LastUpdate", TimestampType(), True),
])
oag_schema = StructType([
    StructField("data", ArrayType(StructType([
        StructField("flightNumber", IntegerType(), True),
        StructField("carrier", StructType([
            StructField("iata", StringType(), True),
            StructField("icao", StringType(), True)
        ]), True),
        StructField("serviceSuffix", StringType(), True),
        StructField("sequenceNumber", IntegerType(), True),
        StructField("flightType", StringType(), True),
        
        StructField("departure", StructType([
            StructField("airport", StructType([
                StructField("iata", StringType(), True),
                StructField("icao", StringType(), True),
                StructField("faa", StringType(), True)
            ]), True),
            StructField("terminal", StringType(), True),
            StructField("date", StructType([
                StructField("local", StringType(), True),
                StructField("utc", StringType(), True)
            ]), True),
            StructField("time", StructType([
                StructField("local", StringType(), True),
                StructField("utc", StringType(), True)
            ]), True),
            StructField("actualTime", StructType([
                StructField("outGateTimeliness", StringType(), True),
                StructField("outGateVariation", StringType(), True),
                StructField("outGate", StructType([
                    StructField("local", StringType(), True),
                    StructField("utc", StringType(), True)
                ]), True),
                StructField("offGround", StructType([
                    StructField("local", StringType(), True),
                    StructField("utc", StringType(), True)
                ]), True)
            ]), True)
        ]), True),

        StructField("arrival", StructType([
            StructField("airport", StructType([
                StructField("iata", StringType(), True),
                StructField("icao", StringType(), True),
                StructField("faa", StringType(), True)
            ]), True),
            StructField("terminal", StringType(), True),
            StructField("date", StructType([
                StructField("local", StringType(), True),
                StructField("utc", StringType(), True)
            ]), True),
            StructField("time", StructType([
                StructField("local", StringType(), True),
                StructField("utc", StringType(), True)
            ]), True),
            StructField("actualTime", StructType([
                StructField("inGateTimeliness", StringType(), True),
                StructField("inGateVariation", StringType(), True),
                StructField("inGate", StructType([
                    StructField("local", StringType(), True),
                    StructField("utc", StringType(), True)
                ]), True),
                StructField("onGround", StructType([
                    StructField("local", StringType(), True),
                    StructField("utc", StringType(), True)
                ]), True)
            ]), True)
        ]), True),

        StructField("statusDetails", ArrayType(StructType([
            StructField("state", StringType(), True),
            StructField("updatedAt", StringType(), True),
            StructField("equipment", StructType([
                StructField("aircraftRegistrationNumber", StringType(), True),
                StructField("actualAircraftType", StructType([
                    StructField("iata", StringType(), True),
                    StructField("icao", StringType(), True)
                ]), True)
            ]), True),
            StructField("departure", StructType([
                StructField("actualTime", StructType([
                    StructField("outGateTimeliness", StringType(), True),
                    StructField("outGateVariation", StringType(), True),
                    StructField("outGate", StructType([
                        StructField("local", StringType(), True),
                        StructField("utc", StringType(), True)
                    ]), True),
                    StructField("offGround", StructType([
                        StructField("local", StringType(), True),
                        StructField("utc", StringType(), True)
                    ]), True)
                ]), True)
            ]), True),
            StructField("arrival", StructType([
                StructField("actualTime", StructType([
                    StructField("inGateTimeliness", StringType(), True),
                    StructField("inGateVariation", StringType(), True),
                    StructField("inGate", StructType([
                        StructField("local", StringType(), True),
                        StructField("utc", StringType(), True)
                    ]), True),
                    StructField("onGround", StructType([
                        StructField("local", StringType(), True),
                        StructField("utc", StringType(), True)
                    ]), True)
                ]), True)
            ]), True)
        ])), True)
    ])), True)
])

def process_data():
    # Load data from JSON files
    adsb_df = spark.read \
        .option("multiline", "true") \
        .json("/app/data/adsb_multi_aircraft.json", schema=adsb_schema)
    oag_df = spark.read \
        .option("multiline", "true") \
        .json("/app/data/oag_multiple.json", schema=oag_schema) \
        .selectExpr("explode(data) as data") \
        .select("data.*")
 
    exploded_df = oag_df.withColumn("statusDetails", explode("statusDetails"))
    delayed_departures = exploded_df.filter(
        col("statusDetails.departure.actualTime.outGateTimeliness") == "Delayed"
    )
    delayed_arrivals = exploded_df.filter(
        col("statusDetails.arrival.actualTime.inGateTimeliness") == "Delayed"
    )
    delayed_departures.show()
    delayed_arrivals.show()

    # Count delayed flights by type
    total_departure_delays = delayed_departures.count()
    total_arrival_delays = delayed_arrivals.count()

    # Combine results into a single DataFrame for saving
    delays_df = spark.createDataFrame([
        ("departure", total_departure_delays),
        ("arrival", total_arrival_delays)
    ], ["DelayType", "TotalDelays"])

    return delays_df

def save_to_db(delays_df):
    jdbc_url = "jdbc:postgresql://db:5432/flight_analyzer"
    properties = {
        "driver": "org.postgresql.Driver",
        "user": "flight_analyzer",
        "password": "flight_analyzer"
    }

    delays_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "delayed_flights") \
        .options(**properties) \
        .mode("overwrite") \
        .save()

def process():
    delays_df = process_data()
    save_to_db(delays_df)

    print("Processing complete. Results saved to the PostgreSQL database.")
