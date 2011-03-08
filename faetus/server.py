import os
import datetime
import time
import mimetypes
import tempfile

from pyftpdlib import ftpserver
from boto.s3.connection import S3Connection
from boto.s3.key import Key


class FaetusOperations(object):
    '''Storing connection object'''
    def __init__(self):
        self.connection = None
        self.username = None
        
    def authenticate(self, username, password):
        self.username = username
        self.connection = S3Connection(username, password)

    
    def __repr__(self):
        return self.connection
    
operations = FaetusOperations()


class FaetusAuthorizer(ftpserver.DummyAuthorizer):
    '''FTP server authorizer. Logs the users into Rackspace Cloud
    Files and keeps track of them.
    '''
    users = {}

    def validate_authentication(self, username, password):
        '''username: your amazon AWS_ACCESS_KEY_ID
        password: your amazon AWS_SECRET_ACCESS_KEY
        '''
        try:
            operations.authenticate(username, password)
            return True
        except:
            return False

    def has_user(self, username):
        return username != 'anonymous'

    def has_perm(self, username, perm, path=None):
        return True

    def get_perms(self, username):
        return 'lrdw'

    def get_home_dir(self, username):
        return os.sep + username

    def get_msg_login(self, username):
        return 'Welcome %s' % username

    def get_msg_quit(self, username):
        return 'Goodbye %s' % username


class FaetusFD(object):

    def __init__(self, username, bucket, obj, mode):
        self.username = username
        self.bucket = bucket
        self.name = obj
        self.mode = mode
        self.closed = False
        self.total_size = 0
        self.temp_file_path = None
        self.temp_file = None

        if not all([username, bucket, obj]):
            self.closed = True
            raise IOError(1, 'Operation not permitted')

        try:
            self.bucket = \
                operations.connection.get_bucket(self.bucket)
        except:
            raise IOError(2, 'No such file or directory')

        if 'r' in self.mode:
            try:
                self.obj = self.bucket.get_key(self.name)
            except:
                raise IOError(2, 'No such file or directory')
        else: #write
            self.obj = self.bucket.get_key(self.name)
            if not self.obj:
                # key does not exist, create it
                self.obj = self.bucket.new_key(self.name)
            # create a temporary file
            self.temp_file_path = tempfile.mkstemp()[1]
            self.temp_file = open(self.temp_file_path, 'w')

    def write(self, data):
        if 'r' in self.mode:
            raise OSError(1, 'Operation not permitted')
        self.temp_file.write(data)
        
    def close(self):
        if 'r' in self.mode:
            return
        self.temp_file.close()
        self.obj.set_contents_from_filename(self.temp_file_path)
        self.obj.close()
        
        # clean up the temporary file
        os.remove(self.temp_file_path)
        self.temp_file_path = None
        self.temp_file = None
    
    def read(self, size=65536):
        return self.obj.read()

    def seek(self, *kargs, **kwargs):
        raise IOError(1, 'Operation not permitted')


class FaetusFS(ftpserver.AbstractedFS):
    '''Amazon S3 File system emulation for FTP server.
    '''

    def parse_fspath(self, path):
        '''Returns a (username, site, filename) tuple. For shorter paths
        replaces not provided values with empty strings.
        '''
        if not path.startswith(os.sep):
            raise ValueError('parse_fspath: You have to provide a full path')
        parts = path.split(os.sep)[1:]
        if len(parts) > 3:
            # join extra 'directories' into key
            parts = parts[0], parts[1], os.sep.join(parts[2:])
        while len(parts) < 3:
            parts.append('')
        return tuple(parts)

    def open(self, filename, mode):
        username, bucket, obj = self.parse_fspath(filename)
        return FaetusFD(username, bucket, obj, mode)

    def chdir(self, path):
        if path.startswith(self.root):
            _, bucket, obj = self.parse_fspath(path)

            if not bucket:
                self.cwd = self.fs2ftp(path)
                return

            if not obj:
                try:
                    operations.connection.get_bucket(bucket)
                    self.cwd = self.fs2ftp(path)
                    return
                except:
                    raise OSError(2, 'No such file or directory')

        raise OSError(550, 'Failed to change directory.')

    def mkdir(self, path):
        try:
            _, bucket, obj = self.parse_fspath(path)
            if obj:
                raise OSError(1, 'Operation not permitted')
        except(ValueError):
            raise OSError(2, 'No such file or directory')

        operations.connection.create_bucket(bucket)

    def listdir(self, path):
        try:
            _, bucket, obj = self.parse_fspath(path)
        except(ValueError):
            raise OSError(2, 'No such file or directory')

        if not bucket and not obj:
            return operations.connection.get_all_buckets()

        if bucket and not obj:
            try:
                cnt = operations.connection.get_bucket(bucket)
                return cnt.list()
            except:
                raise OSError(2, 'No such file or directory')

    def rmdir(self, path):
        _, bucket, name = self.parse_fspath(path)

        if name:
            raise OSError(13, 'Operation not permitted')

        try:
            bucket = operations.connection.get_bucket(bucket)
        except:
            raise OSError(2, 'No such file or directory')

        try:
            operations.connection.delete_bucket(bucket)
        except:
            raise OSError(39, "Directory not empty: '%s'" % bucket)

    def remove(self, path):
        _, bucket, name = self.parse_fspath(path)

        if not name:
            raise OSError(13, 'Operation not permitted')

        try:
            bucket = operations.connection.get_bucket(bucket)
            bucket.delete_key(name)
        except:
            raise OSError(2, 'No such file or directory')
        return not name

    def rename(self, src, dst):
        raise OSError(1, 'Operation not permitted')

    def isfile(self, path):
        return not self.isdir(path)

    def islink(self, path):
        return False

    def isdir(self, path):
        _, _, name = self.parse_fspath(path)
        return not name

    def getsize(self, path):
        return self.stat(path).st_size

    def getmtime(self, path):
        return self.stat(path).st_mtime

    def realpath(self, path):
        return path

    def lexists(self, path):
        try:
            _, bucket, obj = self.parse_fspath(path)
        except(ValueError):
            raise OSError(2, 'No such file or directory')

        if not bucket and not obj:
            buckets = operations.connection.get_all_buckets()
            return bucket in buckets

        if bucket and not obj:
            try:
                cnt = operations.connection.get_bucket(bucket)
                objects = cnt.list()
            except:
                raise OSError(2, 'No such file or directory')
            return obj in objects

    def stat(self, path):
        _, bucket, name = self.parse_fspath(path)
        if not name:
            raise OSError(40, 'unsupported')
        try:
            bucket = operations.connection.get_bucket(bucket)
            obj = bucket.get_key(name)
            return os.stat_result((666, 0L, 0L, 0, 0, 0, obj.size, 0, 0, 0))
        except:
            raise OSError(2, 'No such file or directory')

    exists = lexists
    lstat = stat

    def validpath(self, path):
        return True

    def get_list_dir(self, path):
        try:
            _, bucket, obj = self.parse_fspath(path)
        except(ValueError):
            raise OSError(2, 'No such file or directory')

        if not bucket and not obj:
            buckets = operations.connection.get_all_buckets()
            return self.format_list_buckets(buckets)

        if bucket and not obj:
            try:
                cnt = operations.connection.get_bucket(bucket)
                objects = cnt.list()
            except:
                raise OSError(2, 'No such file or directory')
            return self.format_list_objects(objects)

    def format_list_objects(self, items):
        for item in items:
            ts = datetime.datetime(
                *time.strptime(
                    item.last_modified[:item.last_modified.find('.')],
                    "%Y-%m-%dT%H:%M:%S")[0:6]).strftime("%b %d %H:%M")

            yield '-rw-rw-rw-   1 %s   group  %8s %s %s\r\n' % \
                (operations.username, item.size, ts, item.name)

    def format_list_buckets(self, buckets):
        for bucket in buckets:
            yield 'drwxrwxrwx   1 %s   group  %8s Jan 01 00:00 %s\r\n' % \
                (operations.username, 0, bucket.name)

    def get_stat_dir(self, *kargs, **kwargs):
        raise OSError(40, 'unsupported')

    def format_mlsx(self, *kargs, **kwargs):
        raise OSError(40, 'unsupported')
