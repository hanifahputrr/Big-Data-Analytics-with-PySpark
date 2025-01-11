# -*- coding: utf-8 -*-
"""Data Analysis with PySpark.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1-UyC4AYR_r3f5-CWtZPVWeIhiBO6JT3f
"""

!pip install pyspark

!pip install -U kaleido

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, sum, count
import plotly.express as px

from pyspark.sql import SparkSession
spark_session = SparkSession.builder.appName('customer_analysis').master('local').getOrCreate()

"""Cleaning and Transform Calendar"""

df_calendar = spark_session.read.csv('/content/calendar.csv', header=True)
df_calendar.show(5)

"""Agregasi Tabel Calendar"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, quarter

# Initialize Spark Session
spark = SparkSession.builder.appName("CalendarAggregation").getOrCreate()

# Load the calendar dataset
calendar_df = spark.read.csv("calendar.csv", header=True, inferSchema=True)

# Perform the required transformations
aggregated_calendar_df = calendar_df.withColumn("start_of_the_year", year(col("date"))) \
                                    .withColumn("start_of_the_quarter", quarter(col("date"))) \
                                    .withColumn("start_of_the_month", month(col("date")))

# Show the result
aggregated_calendar_df.show()

"""Define Schema for the Dataframe"""

from pyspark.sql.types import *
schema = StructType([
    StructField("_c0", StringType(), True),
    StructField("lovalty_no", StringType(), True),
    StructField("year", StringType(), True),
    StructField("month", StringType(), True),
    StructField("total_flights", IntegerType(), True),
    StructField("distance", IntegerType(), True),
    StructField("points_accumulated", DoubleType(), True),
    StructField("points_redeemed", IntegerType(), True),
    StructField("dollar_cost_points_redeemed", IntegerType(), True),
])
df_customer_flight_activity = spark_session.read.csv('/content/customer_flight_activity.csv', header=True, schema=schema)
df_customer_flight_activity.show(5)

df_customer_flight_activity = df_customer_flight_activity.drop("_c0")
df_customer_flight_activity.show(5)

"""Cleaning and Trasnform Customer Loyalty Activity"""

from pyspark.sql.types import *
schema = StructType([
    StructField("_c0", StringType(), True),
    StructField("lovalty_no", StringType(), True),
    StructField("country", StringType(), True),
    StructField("province", StringType(), True),
    StructField("city", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("education", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("marital_status", StringType(), True),
    StructField("lolaylty_card", StringType(), True),
    StructField("customer_lifetime_value", IntegerType(), True),
    StructField("enrollment_type", StringType(), True),
    StructField("enrollment_year", StringType(), True),
    StructField("enrollment_month", StringType(), True),
    StructField("cancellation_year", StringType(), True),
    StructField("cancellation_month", StringType(), True),
])
df_customer_loyalty_history = spark_session.read.csv('/content/customer_loyalty_history.csv', header=True, schema=schema)
df_customer_loyalty_history.show(5)

df_customer_loyalty_history = df_customer_loyalty_history.drop("_c0")
df_customer_loyalty_history.show(5)

"""Distribution of Events Over Time"""

from pyspark.sql.functions import year, month
import plotly.express as px

# Aggregate data by year and month
calendar_agg = calendar_df.withColumn("Year", year("date")).withColumn("Month", month("date")) \
    .groupBy("Year", "Month").count().toPandas()

# Plot distribution of events over time
fig1 = px.bar(calendar_agg, x="Month", y="count", color="Year",
              title="Distribution of Events Over Time",
              labels={"count": "Event Count", "Month": "Month"})
fig1.show()
fig1.write_image("distribution of events over time.png")

"""Customer Flight Activity: Flight Patterns"""

from pyspark.sql.functions import col

# Monthly flight counts
flight_monthly = df_customer_flight_activity.groupBy("year", "month") \
    .agg({"total_flights": "sum"}).withColumnRenamed("sum(total_flights)", "TotalFlights").toPandas()

# Scatter plot for distance vs. points accumulated
flight_scatter = df_customer_flight_activity.select(col("distance"), col("points_accumulated")).toPandas()
fig3 = px.scatter(flight_scatter, x="distance", y="points_accumulated",
                  title="Distance vs. Points Accumulated",
                  labels={"distance": "Distance Flown", "points_accumulated": "Points Accumulated"})
fig3.show()
fig3.write_image("distance vs. points accumulated.png")

"""Customer Loyalty History: Loyalty Trends and Demographics"""

from pyspark.sql.functions import avg

# Pie chart for loyalty levels
loyalty_pie = df_customer_loyalty_history.groupBy("lolaylty_card").count().toPandas()
fig4 = px.pie(loyalty_pie, names="lolaylty_card", values="count",
              title="Customer Loyalty Levels Distribution")
fig4.show()

# Average salary by education level
salary_by_education = df_customer_loyalty_history.groupBy("education") \
    .agg(avg("salary").alias("AverageSalary")).toPandas()
fig5 = px.bar(salary_by_education, x="education", y="AverageSalary",
              title="Average Salary by Education Level",
              labels={"education": "Education Level", "AverageSalary": "Average Salary"})
fig5.show()
fig5.write_image("customer loyalty levels distribution.png")