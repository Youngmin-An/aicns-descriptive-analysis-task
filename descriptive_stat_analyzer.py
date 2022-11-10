"""
Descriptive statistics Task
"""

from func import *
from pyspark.sql import SparkSession
from univariate.analyzer import AnalysisReport

if __name__ == "__main__":
    # Initialize app
    app_conf = get_conf_from_evn()

    SparkSession.builder.config(
        "spark.hadoop.hive.exec.dynamic.partition", "true"
    ).config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

    # [AICNS-61]
    if app_conf["SPARK_EXTRA_CONF_PATH"] != "":
        config_dict = parse_spark_extra_conf(app_conf)
        for conf in config_dict.items():
            SparkSession.builder.config(conf[0], conf[1])

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    app_conf["sql_context"] = spark

    # Get feature metadata
    data_col_name = (
        "input_data"  # todo: metadata concern or strict validation column names
    )
    time_col_name = "event_time"

    # Load deduplicated data
    ts = load_deduplicated_data(time_col_name, data_col_name, app_conf)

    # Analyze descriptive statistics
    report: AnalysisReport = analyze_descriptive_stat(ts, time_col_name, data_col_name)

    # save stats
    save_report_stat_to_dwh(report, app_conf)

    spark.stop()
