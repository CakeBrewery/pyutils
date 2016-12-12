import os
from ftp_connection import FTPConnection, FTPConnError
from StringIO import StringIO
import time

CREDENTIALS = ('HOST', 'Your User', 'Hunter2 #memeslol')


def write_file(target_dir, filename, content, credentials=CREDENTIALS):
    """
    Use FTP to move a file to directory.
    :param target_dir: Directory to place file into
    :param filename:
    :param content:
    :param credentials: FTP credentials as tuple(host, user, pw)
    :return: True if succeeded
    """
    with FTPConnection(credentials) as ctn:
        with ctn.directory(target_dir, make_dirs=True):
            return ctn.put_file(filename, StringIO(content))


def sync_directory(source, target):
    """
    Sync entire directory in path, into FTP's target_dir.
    Uses relative path when the path iterator is not the root;
    this avoids the '.' getting joined to FTP path.
    :param source: Source directory to sync
    :param target: Target location to sync to
    """
    for root, dirs, files in os.walk(source):
        for file_ in files:
            dir_ = target
            if root != source:
                dir_ = os.path.join(dir_, os.path.relpath(root, source))

            time.sleep(0.1)  # Give the server some chill
            with open(os.path.join(root, file_), 'rb') as f:
                write_file(dir_, file_, f.read())

