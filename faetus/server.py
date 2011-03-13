import os
import datetime
import ftplib
import time
import mimetypes
import tempfile

from pyftpdlib import ftpserver
from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError, S3CreateError
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from boto.s3.bucketlistresultset import BucketListResultSet

# The path separator used for "virtual directories" in  the cloud system. ("/" for S3)
# This is used for two purposes:
# 1. To recover slashes that pyftpdlib (un)"helpfully" translated to os.sep
# 2. To implement hierarchical keys in S3.
cloud_sep = '/'

# The path separator used by the FTP server.
# Since the FTP server does not have a local filesystem, there little reason to use the OS separator.
# pyftpdlib uses os.sep, though, so we must conform here
ftp_sep = os.sep


# Bit definitions: http://linux.about.com/library/cmd/blcmdl2_stat.htm
FULL_CONTROL_MODE_FLAG = 0600
DIR_MODE_FLAG = 040600
        

def asciify(string):
    # Try to convert string to a legible format for non-Unicode clients.
    try:
        return string.encode('utf-8')
    except:
        return string
            
class FaetusFTPHandler(ftpserver.FTPHandler):
        
    def __init__(self, conn, server):
      super(FaetusFTPHandler, self).__init__(conn, server)
     

class FaetusOperations(object):
    """Storing connection object."""
    def __init__(self):
        self.connection = None
        self.username = None
        
    def authenticate(self, username, password):
        ''       
        self.username = username
        self.connection = S3Connection(username, password)

    
    def __repr__(self):
        return self.connection
    
operations = FaetusOperations()


class FaetusAuthorizer(ftpserver.DummyAuthorizer):
    '''FTP server authorizer and credentials transformer. Logs the users into S3 and keeps track of them.
 In order to provide some "weak" security for shared access to an S3 account,
 transform maps are accepted. These transform maps are use by authenticate,
 to replace the ftp credentials into S3 credentials.
 username_transform_map is of the form {username : aws_access_key_id}
 password_transform_map is of the form {password : aws_secret_access_key}
 Note that due to the nature of S3, AWS_ACCESS_KEY_ID *cannot* be kept secret,
 even if a username_transform_map is defined. A user who knows an FTP user name
 and password can easily recover the AWS_ACCESS_KEY_ID (but not the AWS_SECRET_ACCESS_KEY)
    '''
    users = {}
    
    def __init__(self, allowed_users=None, username_transform_map=None,  password_transform_map=None):
        super(FaetusAuthorizer, self).__init__()
        self.username_transform_map = username_transform_map or {}
        self.password_transform_map = password_transform_map or {}
        self.allowed_users = allowed_users
        if self.allowed_users == []:
          ftpserver.logwarn("Warning: allowed_users is empty. No users can log in!")

    def transform_username(self, username):
        ftpserver.log("transforming username %s" % (username))
        if (self.username_transform_map.has_key(username)):
            username = self.username_transform_map[username]
        ftpserver.log("transformed username to %s" % (username))
        return username
            
    def transform_password(self, password):
        if (self.password_transform_map.has_key(password)):
            password = self.password_transform_map[password]
        return password
    
    def validate_authentication(self, username, password):
        '''username: your amazon AWS_ACCESS_KEY_ID or a mapped username
        password: your amazon AWS_SECRET_ACCESS_KEY or a mapped password
        '''
        try:
            # Check for None, not false here. If the server
            # really wants to allow an empty list of allowed users, 
            # then allow no users.
            if (self.allowed_users is None) or (username in self.allowed_users):
              s3_username = self.transform_username(username)
              s3_password = self.transform_password(password)
              operations.authenticate(s3_username, s3_password)
              return True
            else:
              return False
        except Exception, e:
            ftpserver.logerror(e)
            return False

    def has_user(self, username):
        return username != 'anonymous'

    def has_perm(self, username, perm, path=None):
        return True

    def get_perms(self, username):
        return 'lrdw'

    def get_home_dir(self, username):
        ftpserver.log("get_home_dir(%s(" % (username))
        return ftp_sep + self.transform_username(username)

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
        ftpserver.log("Creating FaetusFD(%s,%s,%s,%s)" %(username, bucket, obj, mode))
        
        if not all([username, bucket, obj]):
            self.closed = True
            raise IOError(1, 'Operation not permitted')

        try:
            self.bucket = operations.connection.get_bucket(self.bucket)
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
        try: 
            self.obj.set_contents_from_filename(self.temp_file_path)
        except S3ResponseError, e:
            # Avoid crashing when the "directory" vanished while we were processing it.
            # This is actually due to a server error. It seems to happen after
            # a "rm file" command incorrectly deletes an entire directory. (!!!)
            ftpserver.logerror("Directory vanished! could not set contents from file %s " % (self.temp_file_path))
            return

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

    # MAX  #objects to display in a bucket, to avoid getting swamped
    # FIXME: implement "virtual directory" support, for keys with '/' in name.
    MAX_OBJECTS = 1000
                
    def __init__(self, root, cmd_channel): super(FaetusFS, self).__init__(root, cmd_channel)

    def get_all_buckets(self):
      try: 
        return list(operations.connection.get_all_buckets())
      except S3ResponseError, e:
        raise OSError(1, "S3 error (probably bad credentials)" + str(e))

    def create_bucket(self, bucket):
      try: 
        return operations.connection.create_bucket(bucket)
      except (S3CreateError, S3ResponseError), e:
        raise OSError(1, "S3 error (probably bucket name conflict)" + str(e))

    def parse_fspath(self, path):
        '''Returns a (username, site, filename) tuple. For shorter paths
        replaces not-provided values with empty strings.
        '''
        ftpserver.log("parse_fspath(%s)" % (path))
        if not path.startswith(ftp_sep):
            raise ValueError('parse_fspath: You have to provide a full path, not %s'  % path)
        parts = path.split(ftp_sep)[1:]
        if len(parts) > 3:
            # join extra 'directories' into key
            # Conveting os.sep (which was unfortunately introduced by pyftpdlib)
            # to cloud_sep
            parts = parts[0], parts[1], cloud_sep.join(parts[2:])
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
                if (self.isdir(path) and self.exists(path)):
                  self._cwd = self.fs2ftp(path)
                else: 
                    raise OSError(550, 'Failed to change directory: Path is not a dir: ' + path)
                return

            if not obj:
                try:
                    operations.connection.get_bucket(bucket)
                    self._cwd = self.fs2ftp(path)
                    return
                except S3ResponseError:
                    raise OSError(550, 'Failed to change directory.')
                    
            raise OSError(550, 'Path is not a dir: ' + obj)

        ftpserver.logerror('Cannot chdir outside of root (%s) to %s' % (self.root, path));
        raise OSError(1, 'Operation not permitted');
                      


    def mkdir(self, path):
        try:
            _, bucket, obj = self.parse_fspath(path)
            if obj:
                raise OSError(1, 'Operation not permitted')
        except(ValueError):
            raise OSError(2, 'No such file or directory')

        self.create_bucket(bucket)
                
    def listdir(self, path):
        """List the content of a directory, as a list of strings."""            

        try:
            _, bucket_name, key_name = self.parse_fspath(path)
        except(ValueError):
            raise OSError(2, 'No such file or directory')
        
        if not bucket_name and not key_name:
            buckets = self.get_all_buckets()
            return map( (lambda bucket: asciify(bucket.name)), buckets)

        if bucket_name and not key_name:
            try:
                bucket = operations.connection.get_bucket(bucket_name)
                # BEWARE! Since S3 does not have native directories
                # this bucket can have arbitrarily many elements.
                # Do NOT convert it to a list!
                # FIXME: implement conventional "virtual directory" support.
                # List only the set of unique prefixes ("virtual directories")
                # http://boto.s3.amazonaws.com/ref/s3.html
                count = 0
                objects_limited = []
                for object in bucket.list(delimiter=cloud_sep):
                    count = count + 1
                    if count > self.MAX_OBJECTS:
                        ftpserver.logerror("Too many items to list! Stopping at #%d" % MAX_OBJECTS)
                        break
                    objects_limited.append(object)
                
                return map( (lambda object: asciify(object.name)), objects_limited)
            except:
                raise OSError(2, 'No such file or directory')

    
    def get_list_dir(self, path):
        """"Return an iterator object that yields a directory listing
        in a form suitable for LIST command.
        """
        try:
            _, bucket, obj = self.parse_fspath(path)
        except(ValueError):
            raise OSError(2, 'No such file or directory')
        
        if not bucket and not obj:
            buckets = self.get_all_buckets()
            return self.format_list_buckets(buckets)

        if bucket and not obj:
            try:
                objects = operations.connection.get_bucket().list(delimiter=cloud_sep)
            except:
                raise OSError(2, 'No such file or directory')
            return self.format_list_objects(objects)

        if bucket and obj:
          # This is a key, which is not supported literally as a directory.
          # Try interpreting as a hierarchical key:
            try:
                objects = operations.connection.get_bucket().list(prefix=obj, delimiter=cloud_sep)
            except:
                raise OSError(2, 'No such file or directory')
            return self.format_list_objects(objects)


    def rmdir(self, path):
        _, bucket, name = self.parse_fspath(path)

        # If the user requests 'rmdir' of a file, refuse that.
        # This is important to avoid falling through to delete an entire bucket!
        if name:
            ftpserver.logerror("RMD requested on (non-drectory) file.")
            raise OSError(13, 'Operation not permitted')

        else:
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
            _, bucket_name, key_name = self.parse_fspath(path)
        except(ValueError):
            raise OSError(2, 'No such file or directory')

        if not bucket_name and not key_name:
            return True # root
            
        if bucket_name and not key_name:
            try:
                bucket = operations.connection.get_bucket(bucket_name)
                objects = bucket.list()
            except:
                raise OSError(2, 'No such file or directory')
            return obj in objects

        if bucket_name and key_name:
            bucket = operations.connection.get_bucket(bucket_name)
            return not (not bucket.get_key(key_name))


    def stat(self, path):

        st_mode = FULL_CONTROL_MODE_FLAG
        _, bucket_name, key_name = self.parse_fspath(path)

        #Hack together a stat result
        
        st_size = 0
        st_mtime = datetime.datetime(  *time.strptime("1970-01-01",  "%Y-%m-%d")[0:6])

        try:
            if not key_name: # Bucket
                # Return a part-bogus stat with the data we do have.
                st_mode = st_mode | DIR_MODE_FLAG
    
            else: # Key
                bucket = operations.connection.get_bucket(bucket_name)
                if (key_name[-1] == cloud_sep): # Virtual directory for hierarchical key.
                    st_mode = st_mode | DIR_MODE_FLAG
                else:
                    obj = bucket.get_key(key_name)
                    # Workaround os.sep crap.
                    if obj is None:
                        obj = bucket.get_key(key_name.replace(cloud_sep, os.sep))
                    if obj is None:
                         ftpserver.logerror("Cannot find object for path %s , key %s in bucket %s " % (path, key_name, bucket_name))
                         raise OSError(2, 'No such file or directory')
                    st_size = obj.size
                   
            return os.stat_result([st_mode, 0, 0, 0, 0, 0, st_size, 0, 0, 0])  #FIXME more stats (mtime)

        
        except Exception,  e:
            ftpserver.logerror("Failed stat(%s) %s %s: %s " % (path, bucket_name, key_name, e))
            raise OSError(2, 'No such file or directory')

    exists = lexists
    lstat = stat

    def validpath(self, path):
        return True

    def format_list_objects(self, items):
        for item in items:
            if item.name[-1] == cloud_sep:
                dir_flag = 'd'
            else:
                dir_flag = '-'
            ts = datetime.datetime(
                *time.strptime(
                    item.last_modified[:item.last_modified.find('.')],
                    "%Y-%m-%dT%H:%M:%S")[0:6]).strftime("%b %d %H:%M")

            yield '%srw------   1 %s   group  %8s %s %s\r\n' % \
                (dir_flag, operations.username, item.size, ts, item.name)

    def format_list_buckets(self, buckets):
        for bucket in buckets:
            name = asciify(bucket.name)
            yield ('drwx------   1 %s   group  %8s Jan 01 00:00 %s\r\n' % \
                   (operations.username, 0, name))
            

    def get_stat_dir(self, *kargs, **kwargs):
        raise OSError(40, 'unsupported')
