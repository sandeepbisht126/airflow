"""
This file is used to get SparkSession to return a valid spark session
"""

from lib.loggingSession import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import sys

def getSparkSession(appName):
	try:
		logger=getloggingSession()
		sc = SparkContext('local')
		spark = SparkSession(sc)
		logger.info("sparkSession.py  ->  Completed Successfully")
		return spark
	except Exception as e:
			logger.info(" Unable to Create Spark Session !!!")
			sys.exit(400)