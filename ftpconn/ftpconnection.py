import io
import logging
from StringIO import StringIO
from ftplib import FTP, error_temp, error_perm, error_reply
from functools import wraps


def connection_required(f):
    """ Decorator for ensuring a connection before calling the wrapped method. """
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        try:
            if not self.connected():
                logging.warning('{}: FTPConnection().start() has not been called. '.format(f.__name__))
                return
            else:
                return f(self, *args, **kwargs)
        except Exception as e:
            # Do something. Notify someone?
            logging.error('{}: Something went wrong. Exception: {}'.format(f.__name__, e))

    return wrapper


class FTPConnection(object):

    def __init__(self, credentials, base_dir='/', *args, **kwargs):
        self.address, self.user, self.password = credentials
        self._connection = FTP(self.address)
        self._base_dir = base_dir

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def connected(self):
        """
        Check if a connection has been started.
        :return: Returns True if self.start() has been called. False otherwise.
            Sometimes pwd() returns AttributeError when connection is closed; Possible bug in ftplib.
        """
        if not self._connection:
            return False

        try:
            self._connection.pwd()  # Quick way to confirm ftp access.
        except (error_temp, error_reply, error_perm, AttributeError) as e:
            return False
        return True

    def start(self):
        """ Initiate connection and switch to base_dir. """
        if self.connected():
            logging.warning('Warning: Aready connected. Calls to FTPConnection.start() more than once will be ignored unless the connection is stopped. ')
            return

        self._connection.login(self.user, self.password)
        self._connection.cwd(self._base_dir)

    @connection_required
    def stop(self, force=True):
        """ Terminate connection with FTP server. """
        try:
            self._connection.quit()  # Politely end connection
        except Exception as e:
            if not force:
                raise e
            self._connection.close()  # Forcefully end connection

    @connection_required
    def list(self):
        return self._connection.nlst()

    @connection_required
    def upload_file(self, filename, contents):
        """
        Upload a file to FTP Server
        :param filename: Name of file to write.
        :param contents: Contents of file to write. Can be String
        :return: True if save succeeded
        """
        if not (filename and contents):
            raise ValueError('Need filename and contents')

        # Temporary filename while storing to ensure no incomplete or halfway-written files get mistaken as batches
        filename_temp = '~{}.temp'.format(filename)

        try:
            file_ = StringIO(contents) if isinstance(contents, (str, basestring)) else contents
            if file_:
                self._connection.storbinary('STOR {}'.format(filename_temp), file_)  # Store with temporary name
                self._connection.rename(filename_temp, filename)  # Once it's complete, give it back its original name

                if filename in self.list():
                    return True

        except Exception as e:
            raise e

    @connection_required
    def get_file(self, filename, dir=None):
        if dir:
            self._connection.cwd(dir)

        if filename in self.list():
            file_ = io.BytesIO()
            self._connection.retrbinary('RETR {}'.format(filename), file_.write)
            file_.seek(0)
            return file_

