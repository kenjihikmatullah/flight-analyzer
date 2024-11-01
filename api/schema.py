from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum, explode
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType

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
        
        StructField("elapsedTime", IntegerType(), True),
        StructField("aircraftType", StructType([          
            StructField("iata", StringType(), True),
            StructField("icao", StringType(), True)
        ]), True),
        StructField("serviceType", StructType([           
            StructField("iata", StringType(), True)
        ]), True),

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