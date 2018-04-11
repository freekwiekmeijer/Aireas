from spark.csv.csv_dialect import CsvDialect
from spark.data import RowFactory

class Parser(object):


    @classmethod
    def parse_row(cls, row):
        columns = row[0].split(CsvDialect.delimiter)
        return RowFactory.get(columns)
