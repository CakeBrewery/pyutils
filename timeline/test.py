from datetime import datetime, timedelta
from timeline import Timeline
import unittest


'''
Attempting to use unittest
'''


def get_timed_data(days=10, interval=1):
    time_pointer = datetime.now()

    data = []

    for i in range(0, days*interval): 
        record = {}
        record['id'] = i*interval
        record['datetime'] = time_pointer

        record['month'] = time_pointer.strftime('%m')
        record['year'] = time_pointer.strftime('%Y')

        # Inesrt data
        data.append(record)

        # Increment time pointer by one day 
        time_pointer = time_pointer - timedelta(days=1)

    return data


class TimelineTest(unittest.TestCase):

    def test_daily(self):
        n_days = 10
        data = get_timed_data(n_days)

        timeline = Timeline(data)
        daily = timeline.split_daily()

        self.assertEqual(len(daily), n_days, msg='Testing with {} days should yield dictionary with {} items'.format(daily, daily))



if __name__ == '__main__':
    unittest.main()


def print_timeline(timeline):
    for elem, value in timeline.iteritems():
        print ('{}: \n {} \n'.format(elem, value))


