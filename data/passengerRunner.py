from lib.logger import Log4j
from lib.SparkSession import getSparkSession
from passenger import *

if __name__ == "__main__":
    #generic module to start Spark session and logging
    spark = getSparkSession("passenger")
    logger = Log4j(spark)
    print("Spark job starts here")

try:
    logger = getloggingSession()
    logger.info("Reading passenger data from source file ...!!!")
    passenger = spark.read.option("header", True).csv("./data/resource/passenger_data.csv")
    #passenger.printSchema()
    #passenger.show()
except Exception as e:
    logger.info("Failed to read passenger data from source file..aborting...!!!")
    sys.exit(400)

passenger_df_casted = cast_passenger_data(passenger)
#passenger_df_casted.printSchema()
#passenger_df_casted.show()

newBooking_df = new_booking_countrywise(passenger_df_casted)
newBooking_df.printSchema()
newBooking_df.show()

try:
    logger = getloggingSession()
    logger.info("Writing resultant dataframe into target file ...!!!")
    newBooking_df.coalesce(1). \
        write. \
        option("header", "true"). \
        mode("overwrite"). \
        csv("./data/result/newPassengersBooking.csv")
except Exception as e:
    logger.info("Failed to write resultant data into target file..aborting...!!!")
    sys.exit(400)
    
spark.stop()