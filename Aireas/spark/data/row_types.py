from datetime import datetime

from spark.csv.csv_dialect import CsvDialect


class BaseRow(object):
    pass


class EmptyRow(BaseRow):
    def __init__(self, *args):
        pass


class SectionHeader(BaseRow):
    def __init__(self, header):
        self.header = header


class Unit(BaseRow):
    def __init__(self, measurement, unit, description):
        self.measurement = measurement
        self.unit = unit
        self.description = description or None


class Location(BaseRow):
    def __init__(self, row_id, country, city, street, zipcode, lat, lon):
        self.id = int(row_id)
        self.country = country
        self.city = city
        self.street = street
        self.zipcode = zipcode
        self.latitude = float(lat)
        self.longitude = float(lon)


class Measurement(BaseRow):
    def __init__(self, loc_id, AmbHum, AmbTemp, lat, lon, NO2, Ozon, PM1, PM10, PM25, RelHum, Temp, UFP, WBGT, type, measured):
        self.location_id = int(loc_id)
        self.amb_humidity = float(AmbHum)
        self.amb_temp = float(AmbTemp)
        self.latitude = float(lat)
        self.longitude = float(lon)
        self.no2 = float(NO2)
        self.o3 = float(Ozon)
        self.particles_pm1 = float(PM1)
        self.particles_pm10 = float(PM10)
        self.particles_pm25 = float(PM25)
        self.rel_humidity = float(RelHum)
        self.temp = float(Temp)
        self.ufp = float(UFP)
        self.wbgt = float(WBGT)
        self.type = type
        self.timestamp = datetime.strptime(measured, CsvDialect.datetime_format)
