from datetime import datetime
from datetime import timedelta


'''
Statistics accumulator. a list of dictionaries dict with dict[date_key] = a datetime object. Accumulate data in
intervals to better discern it.
'''
class Timeline(object):

    def __init__(self, data=None, date_key='datetime'):
        self.date_key = date_key
        self.data = data or []

    def __filter_by(self, date=datetime.today(), compare_string='%Y%m%d'):
        # Get date by datetime
        if isinstance(date, datetime):
            return filter(lambda elem: elem.get(self.date_key) and elem.get(self.date_key).strftime(compare_string) == date.strftime(compare_string), self.data)

        # Get date by string. ie %Y%m%d or %Y%m
        if isinstance(date, (basestring, str)) and len(date) == len(compare_string):
            return filter(lambda elem: elem.get(self.date_key) and elem.get(self.date_key).strftime(compare_string) == date, self.data)

    def __split_by(self, start_date=datetime.min, end_date=datetime.max, compare_string='%Y%m%d'):
        statistics = {}

        for entry in self.filter_by_date(start_date, end_date):
            year_month = entry.get(self.date_key).strftime(compare_string)
            stat = statistics[year_month] if statistics.get(year_month) else {}

            for key in entry.keys():
                if not isinstance(entry[key], datetime):
                    if key not in stat.keys():
                        stat[key] = {entry[key]: 1}
                    else:
                        stat[key][entry[key]] = 1 if entry[key] not in stat[key].keys() else stat[key][entry[key]] + 1

            # Save new/updated stat
            statistics[year_month] = stat

        # Returns dictionary with year_month keys. Each year_month contains a dictionary with data from that month
        return statistics

    # Obtain a subset of the data representing a certain time interval
    def filter_by_date(self, start=datetime.min, end=datetime.max):
        return filter(lambda elem: elem.get(self.date_key) and start <= elem.get(self.date_key) <= end, self.data)

    def filter_by_day(self, date=datetime.today()):
        return self.__filter_by(date, compare_string='%Y%m%d')

    def filter_by_month(self, date=datetime.today()):
        return self.__filter_by(date, compare_string='%Y%m')

    # Split data statistics by month. Search results by results['%Y%m']
    def split_monthly(self, start_date=datetime.min, end_date=datetime.max):
        return self.__split_by(start_date, end_date, '%Y%m')

    def split_daily(self, start_date=datetime.min, end_date=datetime.max):
        return self.__split_by(start_date, end_date, '%Y%m%d')

