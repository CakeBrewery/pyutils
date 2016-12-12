import io
from StringIO import StringIO
from contextlib import contextmanager
from ftplib import FTP, error_temp, error_perm, error_reply
from functools import wraps
import os


class FTPConnError(Exception):
    """ General FTP Connection exception """
    def __init__(self, *args, **kwargs):
        super(FTPConnError, self).__init__(*args, **kwargs)


def connection_required(f):
    """
    Decorator for ensuring a connection before calling the wrapped method.
    Re-raises internal exceptions as FTPConnError exceptions.
    """
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if not self.connected():
            raise FTPConnError('{}: Call FTPConnection().start().'.format(f.__name__))
        else:
            return f(self, *args, **kwargs)

    return wrapper


class FTPConnection(object):

    def __init__(self, credentials, *args, **kwargs):
        self.address, self.user, self.password = credentials
        self._ftp = FTP(self.address)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def connected(self):
        """Returns True if self.start() has been called. False otherwise"""
        if not self._ftp:
            return False

        try:
            self._ftp.pwd()  # Quick way to see if user is connected already.
        except (error_temp, error_reply, error_perm, AttributeError) as e:
            # Sometimes raises AttributeError, might be an ftplib bug.
            return False
        return True

    def start(self):
        """
        Call this before attempting to perform any
        FTP action.
        """
        if self.connected():
            return

        self._ftp.login(self.user, self.password)

    @connection_required
    def _make_dirs(self, path):
        """
        Make all directories in path if
            they don't exist.
        :param path:
        :return: path
        """
        wd = self._ftp.pwd()

        dir_list = path.split(os.path.sep)
        dir_iter = wd

        for dir_ in dir_list:
            dir_iter = os.path.join(dir_iter, dir_)

            if dir_ not in self.list():
                self._ftp.mkd(dir_iter)

        return path

    @connection_required
    @contextmanager
    def directory(self, directory=None, make_dirs=False):
        """
        Context manager for directory switching.
        :param directory: Directory to switch to.
        :param make_dirs: (bool) recursively make
            directories if they don't exist.
        """
        if make_dirs:
            self._make_dirs(directory)

        wd = self._ftp.pwd()
        if directory:
            self._ftp.cwd(directory)
        yield
        self._ftp.cwd(wd)

    @connection_required
    def stop(self):
        """
        End FTP connection. Call this if when done if not using
        the FTPConnection context manager.
        """
        try:
            self._ftp.quit()  # Politely end connection
        except Exception:
            self._ftp.close()  # Forcefully end connection

    @connection_required
    def get_files(self, ext=None):
        """Check for certain file types in the FTP Server"""

        # Switch to appropriate directory
        files = self.list()

        if ext:
            files = filter(lambda filename: filename.endswith(ext), files)

        files_ = []
        for filename in files:
            temp_buffer = StringIO()
            self._ftp.retrbinary('RETR {}'.format(filename), temp_buffer.write)
            files_.append({'File Name': filename, 'Contents': temp_buffer.getvalue()})

        return files_

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

    @connection_required
    def delete_files(self, filenames):
        if isinstance(filenames, list):
            for filename in filenames:
                self.delete_file(filename)
        else:
            self.delete_file(filenames)

    @connection_required
    def list(self):
        return self._ftp.nlst()

    @connection_required
    def put_file(self, filename, file_contents):
        """
        :param filename: Name of file to write.
        :param file_contents: "an open file object which is read until EOF"
                                - Python docs for ftplib.storbinary().
        """
        if file_contents is None or not filename:
            raise ValueError('No file name or contents found.')

        # Temp. name to ensure half-written files don't get mistaken as full files.
        temp_name = '~{}.temp'.format(filename)

        # Store with temporary name
        self._ftp.storbinary('STOR {}'.format(temp_name), file_contents)

        # Once it's complete, give it back its original name
        self._ftp.rename(temp_name, filename)

        if filename in self.list():  # "Assert" file is now in FTP server
            return True
