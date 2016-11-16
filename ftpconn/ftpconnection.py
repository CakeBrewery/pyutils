from StringIO import StringIO
from contextlib import contextmanager
from ftplib import FTP, error_temp, error_perm, error_reply
from functools import wraps


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
            raise FTPConnError('{}: Call LSIConnection().start().'.format(f.__name__))
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
    @contextmanager
    def directory(self, directory=None):
        """
        Context manager for directory switching.
        :param directory: Directory to switch to.
        """
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
        """Check for certain file types in the LSI FTP Server"""

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
