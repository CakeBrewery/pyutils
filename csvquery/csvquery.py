import csv
import decimal


class QueryParameter(object):
    """ Generic query parameter class. Can be overwritten to allow more specific queries parameters. """

    def __init__(self, parameter):
        """
        Args:
            parameter: Exact value to match or a function defining the conditions.
        """
        self.parameter = parameter

    def match(self, value):
        """
        Decides whether value matches the query.
        Overwrite this for custom query parameters.
        :param value: Value to evaluate with this function.
        :return: True or False
        """

        # Allows functions to be passed as well.
        if callable(self.parameter):
            return self.parameter(value)

        return bool(value == self.parameter)

    def __repr__(self):
        return 'QueryParameter: {}'.format(self.parameter)


# Creating custom query example:
class DecimalParameter(QueryParameter):
    """ Search by decimal values. """
    def match(self, value):
        return decimal.Decimal(self.parameter) == decimal.Decimal(value)


class CSVQuery(object):
    """ Query CSV files by their column values. For "small" CSV files when there's no SQLite available. """

    def __init__(self, filename, *args, **kwargs):
        self.filename = filename

    def __normalize_key(self, parameter):
        return parameter.lower().strip().replace(' ', '').replace('/', '')

    def __prepare_query(self, query):
        for key in query.keys():
            if not isinstance(query[key], QueryParameter):
                query[key] = QueryParameter(query[key])

        return query

    def __matches_row(self, row, query=None):
        """Returns true if query parameters match row. False otherwise. """

        # Allow querying through lower-case, space-ommited values, but default to un-normalized key.
        key_getter = { self.__normalize_key(key): key for (key) in row.keys() }

        for param in query:
            if not query[param].match(row.get(key_getter.get(param, param))):
                return False  # Return on first unmatched parameter.

        return True  # All parameters matched

    def search(self, **kwargs):
        """
        Query contents of a CSV file based on parameters specified through keyword arguments.
        To search for a specific column value, use lowercase and ommit spaces.
        ie. csv_query.search(username='John') for a column named 'User Name'.

        Args:
            **kwargs: Column names and values to query.
        Returns: a filter object with the found results.
        """
        with open(self.filename, 'rb') as f:
            reader = csv.DictReader(f)
            self.schema = reader.next()

            # Make kwargs into QueryParameter objects
            query = self.__prepare_query(kwargs)

            return filter(lambda row: self.__matches_row(row, query=query), reader)

    def get(self, **kwargs):
        """ Same as search but stops and returns the first match. """
        with open(self.filename, 'rb') as f:
            reader = csv.DictReader(f)
            self.schema = reader.next()

            # Make kwargs into QueryParameter objects
            query = self.__prepare_query(kwargs)

            return next(row for row in reader if self.__matches_row(row, query))
