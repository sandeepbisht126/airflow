import pyspark.sql.functions as F
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import col, current_date, datediff
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType
cnt=0

def cast_booking_data(bookings):
    booking_df_casted = bookings. \
        withColumn("ts_date_created",(col("date_created")).cast("timestamp")). \
        withColumn("ts_date_close", (col("date_close")).cast("timestamp"))
        #withColumn("dt_date_created", (col("date_created")).cast("date"))
        #drop(col("id")).drop(col("id_passenger"))

    booking_df_casted.printSchema()
    booking_df_casted.show()
    return booking_df_casted

def cast_passenger_data(passenger):
    passenger_df_casted = passenger. \
        withColumn("ts_date_registered", (col("date_registered")).cast("timestamp")). \
        withColumn("dt_date_registered", (col("date_registered")).cast("date")). \
        withColumn("country_code", F.coalesce(col("country_code"), F.lit("OTHER")))

    passenger_df_casted.printSchema()
    passenger_df_casted.show()
    return passenger_df_casted

def new_booking_countrywise(passenger_df_casted):
    newBooking_df = passenger_df_casted.where(datediff(current_date(), col("dt_date_registered")) < 90)
    newBooking_df.show()
    #newBooking_df.select(F.min(col("dt_date_registered"))).alias("min_date").show()    # for testing purpose
    newBooking_df = newBooking_df.groupBy(col("country_code")). \
        agg(F.count(F.lit(1)).alias("Total_newBooking"))
    newBooking_df.show()
    return newBooking_df

def generate_session(diff_time):
    global cnt
    if (diff_time > 5):
        cnt = cnt + 1
    else:
        if (diff_time == 0):
            cnt = 0
    return cnt

def group_passenger_session(booking_df_casted):
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

    final_sessioned_df = sessioned_df.select([col("id_booking"), col("id_passenger"), col("id_session")])
    final_sessioned_df.printSchema()
    final_sessioned_df.show()
    return final_sessioned_df

sc = SparkContext('local')
spark = SparkSession(sc)

print("Spark job starts here")

# TODO Task 1 & 2: Write a process to generate the result data according to the README.md

# @Arena test case in the docker by running
# docker run -it etl-engineer-test_local-airflow
# Task 1
bookings = spark.read.option("header", True).csv("./data/resource/booking_data.csv")
bookings.show()
booking_df_casted = cast_booking_data(bookings)

passenger = spark.read.option("header", True).csv("./data/resource/passenger_data.csv")
passenger.show()
passenger_df_casted = cast_passenger_data(passenger)
newBooking_df = new_booking_countrywise(passenger_df_casted)

newBooking_df.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("./data/result/newPassengersBooking.csv")

# Task 2
final_sessioned_df = group_passenger_session(booking_df_casted)

final_sessioned_df.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("./data/result/PassengersBookingSessions.csv")

spark.stop()

