"""
This file is used to get SparkSession to return a valid spark session
"""

from pyspark.sql import SparkSession
from lib.loggingSession import *
import sys

def getSparkSession(appName):
	try:
		logger=getloggingSession()
		spark=SparkSession.builder.appName(appName).getOrCreate()
		logger.info("sparkSession.py  ->  Completed Successfully")
		return spark
	except Exception as e:
			logger.info(" Unable to Create Spark Session !!!")
			sys.exit(400) 