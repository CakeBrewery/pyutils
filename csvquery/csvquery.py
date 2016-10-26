import itertools, csv


class CSVQuery(object):

    def __init__(self, filename, *args, **kwargs):
        self.filename = filename

    def __normalize_key(self, parameter):
        return parameter.lower().strip().replace(' ', '').replace('/', '')

    def __matches_row(self, row, query=None):
        """Returns true if query parameters match row. False otherwise. """
        key_getter = { self.__normalize_key(key): key for (key) in row.keys() }
        return all([row.get(key_getter.get(x)) == query[x] for x in query])

    def search(self, **kwargs):
        """
        Query contents of a CSV file based on parameters specified through keyword arguments.

        Stores RESULTS in memory. (I tried implementing with itertools.filter to avoid this but
        the csv file must remain open at all times for this to work.)

        To search for a specific column value, use lowercase and ommit spaces.
        ie. csv_query.search(username='John') for a column named 'User Name'. I'm still not sure
        what to do in the case of built-in names.

        Args:
            **kwargs: Column names and values to query.
        Returns: a filter object with the found results.
        """
        with open(self.filename, 'rb') as f:
            reader = csv.DictReader(f)
            self.schema = reader.next()

            return filter(lambda row: self.__matches_row(row, query=kwargs), reader)
