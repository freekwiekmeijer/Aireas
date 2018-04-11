from spark.data.row_types import EmptyRow, Location, Measurement, SectionHeader, Unit


class RowFactory(object):

    @staticmethod
    def determine_empty_or_sectionhead(columns):
        return columns[0].strip() and SectionHeader or EmptyRow

    row_types = {
        1: lambda columns: RowFactory.determine_empty_or_sectionhead(columns),
        3: Unit,
        7: Location,
        16: Measurement
    }

    @classmethod
    def get(cls, columns):
        row_cls = cls.row_types.get(len(columns))

        if not row_cls:
            print ("Unidentified row class, columns {}".format(str(columns)))
            return None

        if row_cls.__class__.__name__ == "function":
            row_cls = row_cls(columns)

        try:
            return row_cls(*columns)
        except:
            pass  # print ("Cannot parse {} object from columns {}".format(row_cls.__name__, str(columns)))
