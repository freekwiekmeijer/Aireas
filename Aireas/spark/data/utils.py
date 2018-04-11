from pyspark.sql import Row


def rdd_to_dataframe(rdd):
    return rdd.map(lambda obj: Row(**obj.__dict__)).toDF()
