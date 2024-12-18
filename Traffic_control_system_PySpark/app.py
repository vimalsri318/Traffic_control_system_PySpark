from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, avg, desc
import pandas as pd
import folium
from folium.plugins import HeatMap

spark = SparkSession.builder.appName("TrafficControlAnalysis").getOrCreate()


file_path = '/Users/macbook/PycharmProjects/Traffic_control_system_PySpark/traffic-control-device-inventory.csv'
df = spark.read.csv(file_path, header=True, inferSchema=True)

print("Schema of the Dataset:")
df.printSchema()
print("First Few Rows:")
df.show(5, truncate=False)

print("Missing Values in Each Column:")
missing_values = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
missing_values.show()

cleaned_df = df.dropna(subset=["Latitude (UTM Y [Meters])", "Longitude (UTM X [Meters])"])

print("Cleaned Dataset Row Count:", cleaned_df.count())
df.show()

device_type_count = cleaned_df.groupBy("Device Type").count().orderBy(desc("count"))
print("Device Type Distribution:")
device_type_count.show()

avg_priority = cleaned_df.groupBy("Maintaining Region").agg(avg("Device Priority").alias("Avg Priority"))
print("Average Device Priority by Region:")
avg_priority.show()

high_priority_devices = cleaned_df.filter(col("Device Priority").contains("SELECTIVE"))
print("High Priority Devices:")
high_priority_devices.show(5)

geo_data = cleaned_df.select("Latitude (UTM Y [Meters])", "Longitude (UTM X [Meters])").limit(100).toPandas()

geo_data["Latitude (UTM Y [Meters])"] = pd.to_numeric(geo_data["Latitude (UTM Y [Meters])"], errors='coerce')
geo_data["Longitude (UTM X [Meters])"] = pd.to_numeric(geo_data["Longitude (UTM X [Meters])"], errors='coerce')

m = folium.Map(location=[geo_data["Latitude (UTM Y [Meters])"].mean(),
                         geo_data["Longitude (UTM X [Meters])"].mean()], zoom_start=10)
df.show()

heat_data = [[row["Latitude (UTM Y [Meters])"], row["Longitude (UTM X [Meters])"]] for _, row in geo_data.iterrows()]
HeatMap(heat_data).add_to(m)
df.show()

m.save("traffic_control_heatmap.html")
print("Map Saved: traffic_control_heatmap.html")
df.show()