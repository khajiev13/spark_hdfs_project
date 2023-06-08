from django.shortcuts import render
import pyspark
import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import to_date
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col, round, date_format
import json

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

bank = spark.read.option("header", "true").csv("hdfs://localhost:9000/hbase/data/holiday_data")
bank_table = bank.createOrReplaceTempView("bank")
# Create your views here.

def hello_view(request):
    return render(request, 'app/base.html')
# Season/ Weather condition / Holidays

def weather_render(request, data):
    data_query = "SELECT MAX(icon) as icon, AVG(temperatureMax) as temperatureMax, AVG(temperatureMin) as temperatureMin, MAX(date_format(sunriseTime, 'HH:mm:ss')) as sunriseTime, MAX(date_format(sunsetTime, 'HH:mm:ss')) as sunsetTime, AVG(humidity) as humidity, AVG(apparentTemperatureHigh) as apparentTemperatureHigh, AVG(apparentTemperatureLow) as apparentTemperatureLow, AVG(energy_sum) as energy_avg, AVG(pressure) as pressure, AVG(windSpeed) as windSpeed FROM (SELECT * FROM daily INNER JOIN w_daily ON daily.day = w_daily.time) where icon = '"+data+"'"
    df = spark.sql(data_query)
    json_data = df.toJSON().collect()
    json_data = json.loads(json_data[0])
    print(json_data)
    return render(request, 'app/weather.html', json_data)

def holidays_render(request):
    import json
    f = open('/home/hadoop/Documents/weather/data.json')
    data = json.load(f)
    for i in data:
        print(i['holiday'])
    f.close()
    return render(request, 'app/holidays.html',{'data': data})

def season_render(request, data):
    data_query = "SELECT * FROM seasons WHERE season = '"+data+"'"
    df = spark.sql(data_query)
    json_data = df.toJSON().collect()
    json_data = json.loads(json_data[0])
    print(json_data)
    return render(request, 'app/season.html', json_data)

from django.shortcuts import render
import json

# def your_view(request):
#     with open('your_json_file.json') as json_file:
#         data = json.load(json_file)
        
#     context = {'data': data}
#     return render(request, 'your_template.html', context)


# {% for item in data %}
#     <p>{{ item.attribute1 }}</p>
#     <p>{{ item.attribute2 }}</p>
#     <!-- Render other attributes as needed -->
# {% endfor %}
