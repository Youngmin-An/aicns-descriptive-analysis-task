"""
Function level adapters
"""
import os
import pendulum
from pyspark.ml.feature import VectorAssembler, MinMaxScalerModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
from univariate.analyzer import Analyzer, DescriptiveStatAnalyzer, AnalysisReport
import logging

__all__ = [
    "get_conf_from_evn",
    "parse_spark_extra_conf",
    "load_deduplicated_data",
    "analyze_descriptive_stat",
    "save_report_stat_to_dwh",
]

logger = logging.getLogger()


def get_conf_from_evn():
    """
        Get conn info from env variables
    :return:
    """
    conf = dict()
    try:
        # Feature id
        conf["FEATURE_ID"] = os.getenv("FEATURE_ID")
        # Raw data period
        start_datetime = os.getenv("APP_TIME_START")  # yyyy-MM-dd'T'HH:mm:ss
        end_datetime = os.getenv("APP_TIME_END")  # yyyy-MM-dd'T'HH:mm:ss
        conf["APP_TIMEZONE"] = os.getenv("APP_TIMEZONE", default="UTC")

        conf["SPARK_EXTRA_CONF_PATH"] = os.getenv(
            "SPARK_EXTRA_CONF_PATH", default=""
        )  # [AICNS-61]
        conf["start"] = pendulum.parse(start_datetime).in_timezone(conf["APP_TIMEZONE"])
        conf["end"] = pendulum.parse(end_datetime).in_timezone(conf["APP_TIMEZONE"])

        # todo: temp patch for day resolution parsing, so later with [AICNS-59] resolution will be subdivided.
        conf["end"] = conf["end"].subtract(minutes=1)

    except Exception as e:
        print(e)
        raise e
    return conf


def parse_spark_extra_conf(app_conf):
    """
    Parse spark-default.xml style config file.
    It is for [AICNS-61] that is spark operator take only spark k/v confs issue.
    :param app_conf:
    :return: Dict (key: conf key, value: conf value)
    """
    with open(app_conf["SPARK_EXTRA_CONF_PATH"], "r") as cf:
        lines = cf.read().splitlines()
        config_dict = dict(
            list(
                filter(
                    lambda splited: len(splited) == 2,
                    (map(lambda line: line.split(), lines)),
                )
            )
        )
    return config_dict


def load_deduplicated_data(time_col_name: str, data_col_name: str, app_conf) -> DataFrame:
    """

    :param time_col_name:
    :param data_col_name:
    :param app_conf:
    :return:
    """
    table_name = "cleaned_deduplicated"
    # Inconsistent cache
    # https://stackoverflow.com/questions/63731085/you-can-explicitly-invalidate-the-cache-in-spark-by-running-refresh-table-table
    SparkSession.getActiveSession().sql(f"REFRESH TABLE {table_name}")
    query = f"""
    SELECT v.{time_col_name}, v.{data_col_name}  
        FROM (
            SELECT {time_col_name}, {data_col_name}, concat(concat(cast(year as string), lpad(cast(month as string), 2, '0')), lpad(cast(day as string), 2, '0')) as date 
            FROM {table_name}
            WHERE feature_id = {app_conf["FEATURE_ID"]}
            ) v 
        WHERE v.date  >= {app_conf['start'].format('YYYYMMDD')} AND v.date <= {app_conf['end'].format('YYYYMMDD')} 
    """
    logger.info("load validated data query: " + query)
    ts = SparkSession.getActiveSession().sql(query)
    logger.debug("validated data")
    ts.show()
    return ts.sort(F.col(time_col_name).desc())


def analyze_descriptive_stat(ts: DataFrame, time_col_name: str, data_col_name: str) -> AnalysisReport:
    """

    :param ts:
    :param time_col_name:
    :param data_col_name:
    :return:
    """
    analyzer: Analyzer = DescriptiveStatAnalyzer()
    report: AnalysisReport = analyzer.analyze(ts=ts, time_col_name=time_col_name, data_col_name=data_col_name)
    logger.info("descriptive stat report: " + str(report.parameters))
    print("descriptive stat report: " + str(report.parameters))
    return report


def save_report_stat_to_dwh(
    report: AnalysisReport, app_conf
):

    # todo: transaction
    table_name = "analyzed_descriptive_stat_estimates"
    SparkSession.getActiveSession().sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} (sample_start TIMESTAMP, sample_end TIMESTAMP, max DOUBLE, min DOUBLE, count INT, mean DOUBLE, median DOUBLE, Q1 DOUBLE, Q3 DOUBLE, stddev DOUBLE, skewness DOUBLE, kurtosis DOUBLE, mode DOUBLE, cv DOUBLE) PARTITIONED BY (feature_id CHAR(10)) STORED AS PARQUET"
    )

    query = f'INSERT INTO {table_name} VALUES(cast({app_conf["start"].timestamp()} as timestamp), cast({app_conf["end"].timestamp()} as timestamp), {report.parameters["max"]}, {report.parameters["min"]}, {report.parameters["count"]}, {report.parameters["mean"]}, {report.parameters["median"]}, {report.parameters["Q1"]}, {report.parameters["Q3"]}, {report.parameters["stddev"]}, {report.parameters["skewness"]}, {report.parameters["kurtosis"]}, {report.parameters["mode"]}, {report.parameters["cv"]}, {app_conf["FEATURE_ID"]})'
    logger.info("stat save query: " + query)
    print("stat save query: " + query)
    SparkSession.getActiveSession().sql(query)
