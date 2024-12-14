from lib.logger import Log4j
from lib.SparkSession import getSparkSession
import sys
from lib.loggingSession import *
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col, current_date, datediff
from pyspark.sql.types import StringType

cnt = 0

def cast_booking_data(bookings):
    try:
        logger = getloggingSession()
        logger.info("Parsing passenger data ...!!!")
        booking_df_casted = bookings. \
            withColumn("ts_date_created",(col("date_created")).cast("timestamp")). \
            withColumn("ts_date_close", (col("date_close")).cast("timestamp"))
            #withColumn("dt_date_created", (col("date_created")).cast("date"))
            #drop(col("id")).drop(col("id_passenger"))
    except Exception as e:
        logger.info("Failed to Parse passenger data..aborting...!!!")
        sys.exit(400)

    return booking_df_casted

def generate_session(diff_time):
    global cnt
    if (diff_time > 5):
        cnt = cnt + 1
    else:
        if (diff_time == 0):
            cnt = 0
    return cnt

def group_passenger_session(booking_df_casted):
    try:
        logger = getloggingSession()
        logger.info("Grouping passenger data ...!!!")
        win_spec = Window.partitionBy("id_passenger").orderBy("date_created")
        my_udf = F.udf(lambda x: generate_session(x), StringType())

        sessioned_df = booking_df_casted. \
            withColumn("id_booking",col("id")) .\
            withColumn("lag_ts_date_created",
                       F.coalesce(F.lag("ts_date_created", 1).over(win_spec), col("ts_date_created"))). \
            withColumn("diff_min",
                       (col("ts_date_created").cast("Bigint") - col("lag_ts_date_created").cast("Bigint")) / 60). \
            withColumn("id_session", F.concat_ws('_', col("id_passenger"), my_udf(col("diff_min")))). \
            sort(col("id_passenger")). \
            sort(col("lag_ts_date_created"))
            #filter(col("id_passenger") == F.lit('257479549'))  # for testing purpose

    except Exception as e:
        logger.info("Failed to group passenger data..aborting...!!!")
        sys.exit(400)

    final_sessioned_df = sessioned_df.select([col("id_booking"), col("id_passenger"), col("id_session")])
    return final_sessioned_df


if __name__ == "__main__":
    #generic module to start Spark session and logging
    spark = getSparkSession("booking")
    logger = Log4j(spark)
    print("Spark job starts here")

try:
    logger = getloggingSession()
    logger.info("Reading booking data from source file ...!!!")
    bookings = spark.read.option("header", True).csv("./data/resource/booking_data.csv")
    #bookings.printSchema()
    #bookings.show()
except Exception as e:
    logger.info("Failed to read booking data from source file..aborting...!!!")
    sys.exit(400)

booking_df_casted = cast_booking_data(bookings)
#booking_df_casted.printSchema()
#booking_df_casted.show()

final_sessioned_df = group_passenger_session(booking_df_casted)
final_sessioned_df.printSchema()
final_sessioned_df.show()

try:
    logger = getloggingSession()
    logger.info("Writing resultant dataframe into target file ...!!!")
    final_sessioned_df.coalesce(1). \
        write. \
        option("header", "true"). \
        mode("overwrite"). \
        csv("./data/result/PassengersBookingSessions.csv")
except Exception as e:
    logger.info("Failed to write resultant data into target file..aborting...!!!")
    sys.exit(400)
    
spark.stop()
