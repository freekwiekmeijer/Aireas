import os

from spark.session import Session


class CsvFile(object):
    measurements_dir = "/media/sf_Share"

    @classmethod
    def _filename(cls, d):
        return "measurements-{}.csv".format(d.strftime("%Y-%m-%d"))

    @classmethod
    def _full_path(cls, d):
        return os.path.join(cls.measurements_dir, cls._filename(d))

    @classmethod
    def as_dataframe(cls, d):
        return Session.get().read.text(cls._full_path(d))