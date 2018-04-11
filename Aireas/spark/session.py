from pyspark.sql import SparkSession


class Session(object):
    @staticmethod
    def get():
        return SparkSession.builder.master("local").appName("Aireas").getOrCreate()