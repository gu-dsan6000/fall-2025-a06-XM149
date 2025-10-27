# problem1.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col
import random
import os
import shutil
import glob

# -------------------------
# Initialize Spark session
# -------------------------
spark = SparkSession.builder.appName("Problem1_LogLevelDistribution").getOrCreate()

# -------------------------
# Load log files
# -------------------------
sample_path = "data/sample/application_1485248649253_0052/*.log" 
full_path = "data/raw/application_*/*.log" 
logs_df = spark.read.text(full_path)  

# -------------------------
# Parse log entries
# -------------------------
parsed_logs = logs_df.select(
    regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
    regexp_extract('value', r'^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\s+(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level'),
    col('value').alias('log_entry')
)

# -------------------------
# Keep only rows with valid log levels
# -------------------------
logs_with_level = parsed_logs.filter(col("log_level").isNotNull() & (col("log_level") != ''))

# -------------------------
# Count log levels
# -------------------------
log_level_counts = logs_with_level.groupBy('log_level').count().orderBy('count', ascending=False)
log_level_counts.show()

# -------------------------
# Save counts as a single CSV file
# -------------------------
tmp_path = "data/output/tmp_problem1_counts"
output_file = "data/output/problem1_counts.csv"

log_level_counts.coalesce(1).write.csv(tmp_path, header=True, mode='overwrite')

part_file = glob.glob(os.path.join(tmp_path, "part-*.csv"))[0]
shutil.move(part_file, output_file)
shutil.rmtree(tmp_path) 

# -------------------------
# Sample 10 random log entries with levels
# -------------------------
sampled_logs = logs_with_level.sample(False, 0.01) 
sample_list = sampled_logs.collect()
if len(sample_list) > 10:
    sample_list = random.sample(sample_list, 10)

with open("data/output/problem1_sample.csv", "w") as f:
    f.write("log_entry,log_level\n")
    for row in sample_list:
        entry = row.log_entry.replace(",", "，")
        f.write(f'"{entry}",{row.log_level}\n')

# -------------------------
# Summary statistics
# -------------------------
total_lines = logs_df.count()
total_with_levels = logs_with_level.count()
unique_levels = logs_with_level.select("log_level").distinct().count()

level_stats = {row['log_level']: row['count'] for row in log_level_counts.collect()}

with open("data/output/problem1_summary.txt", "w") as f:
    f.write(f"Total log lines processed: {total_lines}\n")
    f.write(f"Total lines with log levels: {total_with_levels}\n")
    f.write(f"Unique log levels found: {unique_levels}\n\n")
    f.write("Log level distribution:\n")
    for level in ["INFO", "WARN", "ERROR", "DEBUG"]:
        count = level_stats.get(level, 0)
        pct = count / total_with_levels * 100 if total_with_levels > 0 else 0
        f.write(f"  {level:5}: {count:10} ({pct:6.2f}%)\n")

# -------------------------
# Done
# -------------------------
spark.stop()
print("Problem 1 completed successfully!")
