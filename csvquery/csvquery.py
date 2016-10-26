import itertools, csv


class CSVQuery(object):

    def __init__(self, csv_filename):
        self.rows = []
        self.schema = None

        with open(csv_filename, 'rb') as f:
            reader = csv.DictReader(f)
            self.schema = reader.next()

            for row in reader:
                new_entry = {}
                for key in row.keys():
                    new_entry[self.__normalize_key(key)] = row[key]
                    self.rows.append(new_entry)

    def __normalize_key(self, parameter):
        return parameter.lower().strip().replace(' ', '').replace('/', '')

    def __matches_row(self, row, query=None):
        """Returns true if query parameters match row. False otherwise. """
        return all([row.get(self.__normalize_key(x), '') == query[x] for x in query])

    def search(self, *args, **kwargs):
        """ Search using keyword arguments """
        return itertools.ifilter(lambda row: self.__matches_row(row, query=kwargs), self.rows)
