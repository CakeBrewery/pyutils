from datetime import datetime
from datetime import timedelta


'''
Statistics accumulator. a list of dictionaries dict with dict[date_key] = a datetime object. Accumulate data in
intervals to better discern it.
'''
class Timeline(object):

    def __init__(self, data=None, date_key='datetime'):
        self.date_key = date_key
        self._data = data or []

    def _filter_by(self, date=datetime.today(), compare_string='%Y%m%d'):
        # Get date by datetime
        if isinstance(date, datetime):
            return filter(lambda elem: elem.get(self.date_key) and elem.get(self.date_key).strftime(compare_string) == date.strftime(compare_string), self._data)

        # Get date by string. ie %Y%m%d or %Y%m
        if isinstance(date, (basestring, str)) and len(date) == len(compare_string):
            return filter(lambda elem: elem.get(self.date_key) and elem.get(self.date_key).strftime(compare_string) == date, self._data)

    def _split_by(self, start_date=datetime.min, end_date=datetime.max, compare_string='%Y%m%d'):
        statistics = {}

        for entry in self.filter_by_date(start_date, end_date):
            date_pointer = entry.get(self.date_key).strftime(compare_string)
            stat = statistics[date_pointer] if statistics.get(date_pointer) else {}

            for key in entry.keys():
                if not isinstance(entry[key], datetime):
                    if key not in stat.keys():
                        stat[key] = {entry[key]: 1}
                    else:
                        stat[key][entry[key]] = 1 if entry[key] not in stat[key].keys() else stat[key][entry[key]] + 1

            # Save new/updated stat
            statistics[date_pointer] = stat

        # Return a dictionary of keys of the format compare_string. for example: statistics['%Y%m'] where compare_string is '%Y%m' of a specified date
        return statistics

    @property
    def data():
        return self._data

    # Obtain a subset of the data representing a certain time interval
    def filter_by_date(self, start=datetime.min, end=datetime.max):
        return filter(lambda elem: elem.get(self.date_key) and start <= elem.get(self.date_key) <= end, self._data)

    def filter_by_day(self, date=datetime.today()):
        return self._filter_by(date, compare_string='%Y%m%d')

    def filter_by_month(self, date=datetime.today()):
        return self._filter_by(date, compare_string='%Y%m')

    # Split data statistics by day. Search results by results['%Y%m%d']
    def split_daily(self, start_date=datetime.min, end_date=datetime.max):
        return self._split_by(start_date, end_date, '%Y%m%d')

    # Split data statistics by month. Search results by results['%Y%m']
    def split_monthly(self, start_date=datetime.min, end_date=datetime.max):
        return self._split_by(start_date, end_date, '%Y%m')

