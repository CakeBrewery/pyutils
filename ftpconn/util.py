import os
from ftp_connection import FTPConnection, FTPConnError
from StringIO import StringIO
import time

CREDENTIALS = ('HOST', 'Your User', 'Hunter2 #memeslol')


def put_file(target_dir, filename, content, credentials=CREDENTIALS):
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


def sync_directory(path, target_dir):
    """
    Sync entire directory in path, into FTP's target_dir.
    Uses relative path when the path iterator is not the root;
    this avoids the '.' getting joined to FTP path.
    :param path: Path of dir to sync
    :param target_dir: Target location in FTP
    """
    for root, dirs, files in os.walk(path):
        for file_ in files:

            # Give the server some chill
            time.sleep(0.1)

            file_path = os.path.join(root, file_)

            if root == path:
                new_dir = target_dir
            else:
                rel = os.path.relpath(root, path)
                new_dir = os.path.join(target_dir, rel)

            with open(file_path, 'rb') as f:
                put_file(new_dir, file_, f.read())
