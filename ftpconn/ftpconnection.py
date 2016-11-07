import io
import logging
from ftplib import FTP, error_temp, error_perm, error_reply
from functools import wraps
from contextlib import contextmanager


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
            # TODO: Find a way to handle this
            logging.error('{}: Something went wrong. Exception: {}'.format(f.__name__, e))

    return wrapper


class FTPConnection(object):

    def __init__(self, credentials, base_dir='/', *args, **kwargs):
        self.address, self.user, self.password = credentials
        self._ftp = FTP(self.address)
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
        if not self._ftp:
            return False

        try:
            self._ftp.pwd()  # Quick way to confirm ftp access.
        except (error_temp, error_reply, error_perm, AttributeError) as e:
            return False
        return True

    def start(self):
        """ Initiate connection and switch to base_dir. """
        if self.connected():
            logging.warning('Warning: Aready connected. Calls to FTPConnection.start() more than once will be ignored unless the connection is stopped. ')
            return

        self._ftp.login(self.user, self.password)
        self._ftp.cwd(self._base_dir)

    @connection_required
    def stop(self, force=True):
        """ Terminate connection with FTP server. """
        try:
            self._ftp.quit()  # Politely end connection
        except Exception as e:
            if not force:
                raise e
            self._ftp.close()  # Forcefully end connection

    @connection_required
    @contextmanager
    def directory(self, directory=None):
        wd = self._ftp.pwd()
        if directory:
            self._ftp.cwd(directory)
        yield
        self._ftp.cwd(wd)

    @connection_required
    def list(self):
        return self._ftp.nlst()

    @connection_required
    def put_file(self, filename, contents):
        """
        Upload a file to FTP Server
        :param filename: Name of file to write.
        :param contents: Contents of file to write. Can be String
        :return: True if save succeeded
        """
        if not (filename and contents):
            raise ValueError('Need filename and contents')

            # Temp. name to ensure half-written files don't get mistaken as full files.
        temp_name = '~{}.temp'.format(filename)

        # Store with temporary name
        self._ftp.storbinary('STOR {}'.format(temp_name), contents)

        # Once it's complete, give it back its original name
        self._ftp.rename(temp_name, filename)

        if filename in self.list():
            return True

    @connection_required
    def get_file(self, filename):
        if filename in self.list():
            file_ = io.BytesIO()
            self._ftp.retrbinary('RETR {}'.format(filename), file_.write)
            file_.seek(0)
            return file_

    @connection_required
    def delete_file(self, filename):
        # Delete file from FTP Server
        self._ftp.delete(filename)
