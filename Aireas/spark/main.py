from datetime import date

from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf

from spark.csv import CsvFile, Parser
from spark.data.utils import rdd_to_dataframe


if __name__ == "__main__":
    d = date(2018, 4, 10)

    # Load DataFrame from CSV file
    df = CsvFile.as_dataframe(d)

    # Parse all rows into their respective types (CSV has different sections with different data types)
    # TODO: why go from DF to RDD and back???
    parsed = df.rdd.map(Parser.parse_row)
    measurements = rdd_to_dataframe(parsed.filter(lambda obj: obj.__class__.__name__ == "Measurement"))
    locations = rdd_to_dataframe(parsed.filter(lambda obj: obj.__class__.__name__ == "Location"))

    # Join measurements on locations
    joined = measurements.join(locations, measurements.location_id == locations.id)
    print("Measurements: {}").format(joined.count())

    # Filter by city
    filtered = joined.where(joined.city == "Eindhoven")
    print("Eindhoven measurements: {}").format(filtered.count())

    # Add column "particles_total" using user-defined function
    def sum_columns(col1, col2, col3):
        return col1+col2+col3
    sum_columns_udf = udf(sum_columns, FloatType())

    filtered = filtered.withColumn(
        "particles_total",
        sum_columns_udf(
            filtered["particles_pm1"],
            filtered["particles_pm10"],
            filtered["particles_pm25"]
        )
    )

    # Group by timestamp and take the average particles of all measurements for the same timestamp (all locations)
    grouped = filtered.groupBy("timestamp").agg({
        "particles_pm1": "mean",
        "particles_pm10": "mean",
        "particles_pm25": "mean",
        "particles_total": "mean"
    })

    out = grouped.orderBy("timestamp").select("timestamp", "avg(particles_total)")
    out.show(out.count(), False)


# TODO meerdere dagen
## meerdere CSV files, union dataframes, locations.dictinct()
## calculated column "timeofday" of grouping te kunnen doen van hetzelfde tijdstip op meerdere dagen
