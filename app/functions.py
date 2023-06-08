import pyspark
import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import to_date
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col, round


def stop_spark():
    spark.stop()

def weather_get(icon):
    data_query = "SELECT AVG(temperatureMax) as temperatureMax, AVG(temperatureMin) as temperatureMin, MAX(sunriseTime) as sunriseTime, MAX(sunsetTime) as sunsetTime, AVG(humidity) as humidity, AVG(apparentTemperatureHigh) as apparentTemperatureHigh, AVG(apparentTemperatureLow) as apparentTemperatureLow, AVG(energy_sum) as energy_avg FROM (SELECT * FROM daily INNER JOIN w_daily ON daily.day = w_daily.time) where icon = '"+icon+"'"
    
    df = spark.sql(data_query)
    for i in ['windSpeed', 'humidity', 'pressure']:
        df = df.withColumn(i, round(col(i), 3))
    df.show()
    return df.collect()

def season_get(season):
    data_query = "SELECT * FROM seasons WHERE season = '"+season+"'"
    df = spark.sql(data_query)
    df.show()
    

spark = SparkSession.builder.appName("London Energy Usage").getOrCreate()

daily_df = spark.read.option("header", "true").csv("hdfs://localhost:9000/hbase/data/daily")
daily_df = daily_df.withColumn("day", to_date(daily_df["day"]))
daily = daily_df.createOrReplaceTempView("daily")

weather_daily = spark.read.option("header", "true").csv("hdfs://localhost:9000/hbase/data/weather/daily")
weather_daily = weather_daily.withColumn("time", to_date(weather_daily["time"]))
w_daily = weather_daily.createOrReplaceTempView("w_daily")

seasons = spark.read.csv("hdfs://localhost:9000/hbase/data/seasons_data")
season_columns = ["season", "average_usage", "record_max", "record_low"]
seasons = seasons.toDF(*season_columns)
seasons_table = seasons.createOrReplaceTempView("seasons")
seasons.show()

bank = spark.read.option("header", "true").csv("hdfs://localhost:9000/hbase/data/holiday_data")
bank_table = bank.createOrReplaceTempView("bank")

cloudy = weather_get("cloudy")
winter = season_get("winter")

stop_spark()