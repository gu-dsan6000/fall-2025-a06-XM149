import os
import shutil
import glob
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, input_file_name, col, min as spark_min, max as spark_max, row_number
from pyspark.sql.functions import try_to_timestamp, lit
from pyspark.sql.window import Window

# -------------------------------
# Configuration
# -------------------------------
sample_path = "data/raw/application_*/*.log"  
output_dir = "data/output"
os.makedirs(output_dir, exist_ok=True)

# -------------------------------
# Spark Session (local mode)
# -------------------------------
spark = SparkSession.builder \
    .appName("Cluster Usage Analysis - Local Sample") \
    .master("local[*]") \
    .getOrCreate()

# -------------------------------
# Read log files
# -------------------------------
logs_df = spark.read.text(sample_path)

# -------------------------------
# Extract timestamp, log_level, log_entry
# -------------------------------
parsed_logs = logs_df.select(
    regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
    regexp_extract('value', r'^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\s+(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level'),
    col('value').alias('log_entry'),
    input_file_name().alias('file_path')
)

# -------------------------------
# Extract application_id and cluster_id
# -------------------------------
df = parsed_logs.withColumn(
    "application_id", regexp_extract(col("file_path"), r"(application_\d+_\d+)", 1)
).withColumn(
    "cluster_id", regexp_extract(col("application_id"), r"(\d+)_\d+", 1)
).withColumn(
    "timestamp", try_to_timestamp(col("timestamp"), lit("yy/MM/dd HH:mm:ss"))
)

# -------------------------------
# Compute start_time and end_time per application (Spark)
# -------------------------------
timeline_spark = df.groupBy("cluster_id", "application_id").agg(
    spark_min("timestamp").alias("start_time"),
    spark_max("timestamp").alias("end_time")
)

# Add app_number per cluster
window = Window.partitionBy("cluster_id").orderBy("start_time")
timeline_spark = timeline_spark.withColumn("app_number", row_number().over(window))

# -------------------------------
# Save timeline CSV (single file)
# -------------------------------
tmp_timeline = os.path.join(output_dir, "tmp_timeline")
timeline_spark.select("cluster_id", "application_id", "app_number", "start_time", "end_time") \
    .coalesce(1) \
    .write.csv(tmp_timeline, header=True, mode="overwrite")
part_file = glob.glob(os.path.join(tmp_timeline, "part-*.csv"))[0]
shutil.move(part_file, os.path.join(output_dir, "problem2_timeline.csv"))
shutil.rmtree(tmp_timeline)

# -------------------------------
# Cluster summary (Spark)
# -------------------------------
cluster_summary_spark = timeline_spark.groupBy("cluster_id").agg(
    spark_min("start_time").alias("cluster_first_app"),
    spark_max("end_time").alias("cluster_last_app"),
    col("cluster_id")  # retain cluster_id
)

# Add num_applications per cluster
num_apps_spark = timeline_spark.groupBy("cluster_id").count().withColumnRenamed("count", "num_applications")
cluster_summary_spark = cluster_summary_spark.join(num_apps_spark, on="cluster_id")

# -------------------------------
# Save cluster summary CSV (single file)
# -------------------------------
tmp_cluster = os.path.join(output_dir, "tmp_cluster")
cluster_summary_spark.select("cluster_id", "num_applications", "cluster_first_app", "cluster_last_app") \
    .coalesce(1) \
    .write.csv(tmp_cluster, header=True, mode="overwrite")
part_file = glob.glob(os.path.join(tmp_cluster, "part-*.csv"))[0]
shutil.move(part_file, os.path.join(output_dir, "problem2_cluster_summary.csv"))
shutil.rmtree(tmp_cluster)

# -------------------------------
# Statistics (Spark -> Python)
# -------------------------------
stats_data = cluster_summary_spark.select("cluster_id", "num_applications").collect()
total_clusters = len(stats_data)
total_apps = sum(row["num_applications"] for row in stats_data)
avg_apps_per_cluster = total_apps / total_clusters
most_used = sorted(stats_data, key=lambda x: x["num_applications"], reverse=True)

with open(os.path.join(output_dir, "problem2_stats.txt"), "w") as f:
    f.write(f"Total unique clusters: {total_clusters}\n")
    f.write(f"Total applications: {total_apps}\n")
    f.write(f"Average applications per cluster: {avg_apps_per_cluster:.2f}\n\n")
    f.write("Most heavily used clusters:\n")
    for row in most_used:
        f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")

# -------------------------------
# Visualizations
# -------------------------------
# Convert minimal needed data to Pandas for plotting
timeline_pd = timeline_spark.select("cluster_id", "application_id", "start_time", "end_time").toPandas()
cluster_summary_pd = cluster_summary_spark.select("cluster_id", "num_applications").toPandas()

sns.set(style="whitegrid")

# Bar chart - number of applications per cluster
plt.figure(figsize=(8, 5))
sns.barplot(data=cluster_summary_pd, x="cluster_id", y="num_applications", palette="Set2")
plt.title("Number of applications per cluster")
plt.xlabel("Cluster ID")
plt.ylabel("Number of applications")
for i, row in cluster_summary_pd.iterrows():
    plt.text(i, row["num_applications"], str(row["num_applications"]), ha="center", va="bottom")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "problem2_bar_chart.png"))
plt.close()

# Density plot - job duration for largest cluster
largest_cluster = cluster_summary_pd.sort_values("num_applications", ascending=False).iloc[0]["cluster_id"]
subset = timeline_pd[timeline_pd["cluster_id"] == largest_cluster].copy()
subset["duration_sec"] = (subset["end_time"] - subset["start_time"]).dt.total_seconds()

plt.figure(figsize=(8,5))
sns.histplot(subset, x="duration_sec", kde=True, log_scale=True)
plt.title(f"Job duration distribution for cluster {largest_cluster} (n={len(subset)})")
plt.xlabel("Duration (seconds, log scale)")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "problem2_density_plot.png"))
plt.close()

print("Problem 2 analysis done! Outputs saved in data/output/")
