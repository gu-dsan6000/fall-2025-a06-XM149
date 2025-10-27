# Analysis Report

## Brief description of your approach for each problem

### Problem 1 – Log Level Distribution and Sampling

For Problem 1, we used PySpark to process all raw log files across the dataset. The approach consisted of the following steps:

Log Parsing: Each log line was parsed using regular expressions to extract the timestamp, log level (INFO, WARN, ERROR, DEBUG), and the full log entry.

Filtering: Only rows with valid log levels were retained to ensure accurate statistics.

Aggregation: We counted the occurrences of each log level using Spark’s groupBy and count functions.

Output: Results were saved as single CSV files to avoid multiple-part Spark outputs. A small random sample of log entries with levels was collected and saved separately for inspection.

Summary Statistics: Total log lines, lines with log levels, unique levels, and log level distributions were computed and saved as a text file.

This approach leverages Spark’s distributed processing to handle large log datasets efficiently without overloading local memory.

### Problem 2 – Cluster Usage Timeline and Visualization

For Problem 2, we focused on analyzing application execution timelines across clusters:

Log Parsing and Metadata Extraction: Similar to Problem 1, we extracted timestamp, log level, and log entry. Additionally, we extracted application_id and cluster_id from the log file paths.

Timeline Computation: For each application, the start and end times were computed using Spark aggregations (min and max on timestamp).

Application Numbering: Applications were numbered per cluster based on start time using Spark’s window functions.

Cluster Summary: For each cluster, we computed the total number of applications, as well as the first and last application times.

Output: Both the timeline and cluster summary were written as single CSV files to simplify downstream analysis.

Statistics and Visualizations: We collected minimal summary data into Python to compute overall statistics (total clusters, average applications per cluster, most used clusters) and generate visualizations:

Bar chart of number of applications per cluster

Histogram with KDE of job durations for the largest cluster

This approach ensures that heavy data processing is performed in Spark, minimizing memory consumption, while Pandas is only used for small-scale summary and visualization tasks.

## Key findings and insights from the data

### Problem 1 – Log Level Analysis

The dataset contains 33,236,604 log lines, of which 27,410,250 lines (82.5%) have valid log levels.

Only 3 log levels were found: INFO, WARN, and ERROR.

The INFO level dominates overwhelmingly with 27,389,472 entries (99.92%), while WARN and ERROR are extremely rare (0.04% each).

This indicates that the system generates predominantly informational messages, and warnings/errors are exceptional events.

A small sample of log entries was also saved to provide concrete examples of typical logs for each level.

### Problem 2 – Cluster Usage Analysis

There are 6 unique clusters and 194 applications in total.

The distribution of applications is highly skewed: Cluster 1485248649253 is by far the busiest with 181 applications, while other clusters have between 1 and 8 applications only.

The average applications per cluster is 32.33, but this is inflated by the busiest cluster.

Job durations (shown in the histogram) span multiple orders of magnitude, with many jobs finishing in under 1,000 seconds, but a long tail extending to tens of thousands of seconds. This is why a log scale is used in the visualization.

These patterns indicate a highly uneven utilization of clusters, with one cluster handling the majority of workloads.

## Performance observations (execution time, optimizations)

Spark’s distributed computation allows efficient processing of tens of millions of log lines without exceeding memory limits.

Writing outputs as single CSV files (using coalesce(1) + temporary folder) avoids creating multiple Spark part-*.csv files while keeping memory usage manageable.

Minimal use of Pandas is applied only for summarization and plotting, which prevents local memory overflow.

Sampling of log entries is done using Spark’s .sample() function, avoiding the need to collect the entire dataset to the driver.

Overall, both problems completed quickly in local Spark mode due to efficient aggregation and minimal shuffling.

Screenshots of Spark Web UI showing job execution


## Explanation of the visualizations generated in Problem 2

### Bar chart – Number of applications per cluster:

Shows the distribution of applications across clusters.

Clearly highlights that Cluster 1485248649253 handles the majority of workloads, while others have very few.

### Histogram with KDE – Job duration distribution (log scale) for the largest cluster:

The x-axis is in log scale to accommodate the wide range of job durations.

Most jobs complete relatively quickly, but there is a long tail of longer-running jobs.

KDE curve shows the smoothed density, confirming that most jobs finish within the lower duration range, while a few extreme jobs extend much longer.