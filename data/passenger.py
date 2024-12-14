import sys
from lib.loggingSession import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col, current_date, datediff

def cast_passenger_data(passenger):
    try:
        logger = getloggingSession()
        logger.info("Parsing passenger data ...!!!")
        passenger_df_casted = passenger. \
            withColumn("ts_date_registered", (col("date_registered")).cast("timestamp")). \
            withColumn("dt_date_registered", (col("date_registered")).cast("date")). \
            withColumn("country_code", F.coalesce(col("country_code"), F.lit("OTHER")))
    except Exception as e:
        logger.info("Failed to Parse passenger data..aborting...!!!")
        sys.exit(400)

    logger.info("Input File Successfully read into a DataFrame!!!")
    return passenger_df_casted

def new_booking_countrywise(passenger_df_casted):
    try:
        logger = getloggingSession()
        logger.info("Calculating Total booking by new passengers by country of origin ...!!!")
        newBooking_df = passenger_df_casted.where(datediff(current_date(), col("dt_date_registered")) < 90)
        newBooking_df.show()
        #newBooking_df.select(F.min(col("dt_date_registered"))).alias("min_date").show()    # for testing purpose
        newBooking_df = newBooking_df.groupBy(col("country_code")). \
            agg(F.count(F.lit(1)).alias("Total_newBooking"))
    except Exception as e:
        logger.info("Failed to calculate booking by new passengers by country of origin..aborting...!!!")
        sys.exit(400)

    logger.info("Successfully calculated total booking by new passengers by country by origin!!!")
    return newBooking_df