#encoding=utf-8

from django.core.files.storage import Storage
from django.conf import settings
from django.core.files.base import File
from django.utils import importlib
from qiniu import conf
from qiniu import io
from qiniu import rs
import os
import urllib2
import itertools

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

ACCESS_KEY = getattr(settings, 'QINIU_ACCESS_KEY', None)
SECRET_KEY = getattr(settings, 'QINIU_SECRET_KEY', None)
BUCKET_KEY = getattr(settings, 'QINIU_BUCKET_KEY', None)
BUCKET_HOST = getattr(settings, 'QINIU_BUCKET_HOST', None)
ENCRYPT_FUNC = getattr(settings, 'QINIU_ENCRYPT_FUNC', None)
IS_PRIVATE_BUCKET = getattr(settings, 'QINIU_BUCKET_IS_PRIVATED', False)


class QiniuStorage(Storage):

    def __init__(self, access_key=ACCESS_KEY, secret_key=SECRET_KEY,
                 bucket=BUCKET_KEY, is_private=IS_PRIVATE_BUCKET,
                 bucket_host=BUCKET_HOST, encrypt_func=ENCRYPT_FUNC):
        self.access_key = access_key
        self.secret_key = secret_key
        conf.ACCESS_KEY = access_key
        conf.SECRET_KEY = secret_key
        self.bucket = bucket
        self.is_private = is_private
        self.bucket_host = bucket_host

        if not encrypt_func:
            self.encrypt_func = None
            return

        if callable(encrypt_func):
            self.encrypt_func = encrypt_func
            return

        try:
            parts = encrypt_func.split('.')
            module_path, class_name = '.'.join(parts[:-1]), parts[-1]
            module = importlib.import_module(module_path)
            self.encrypt_func = getattr(module, class_name)
        except ImportError as e:
            msg = "Could not import '%s' for API setting '%s'. %s: %s." %\
                (encrypt_func, 'QINIU_ENCRYPT_FUNC', e.__class__.__name__, e)
            raise ImportError(msg)

    def _open(self, name, mode='rb'):
        name = self._clean_name(name)
        remote_file = QiniuFile(name, self, mode=mode)
        return remote_file

    def _read(self, name, start_range=None, end_range=None):
        # 支持断点下载，仅供QiniuFile使用，后两个参数需要同时传。
        name = self._clean_name(name)

        if not start_range:
            headers = {}
        else:
            headers = {'Range': 'bytes=%s-%s' % (start_range, end_range)}
        request = urllib2.Request(self.url(name))
        request.headers.update(headers)
        f = urllib2.urlopen(request)
        data = f.read()
        if not start_range and self.encrypt_func:
            data = self.encrypt_func(data, decrypt=True)

        return data, f.headers.get('content-range')

    def _clean_name(self, name):
        return os.path.normpath(name).replace('\\', '/')

    def _save(self, name, content):
        name = self._clean_name(name)
        content.open()
        if hasattr(content, 'chunks'):
            content_str = ''.join(chunk for chunk in content.chunks())
        else:
            content_str = content.read()
        self._put_file(name, content_str)
        return name

    def save(self, name, content):
        name = self.get_available_name(name)
        return self._save(name, content)


    def get_available_name(self, name):
        """
        Returns a filename that's free on the target storage system, and
        available for new content to be written to.
        """
        file_root, file_ext = os.path.splitext(name)
        # If the filename already exists, add an underscore and a number (before
        # the file extension, if one exists) to the filename until the generated
        # filename doesn't exist.
        count = itertools.count(1)
        while self.exists(name):
            # file_ext includes the dot.
            name = "%s_%s%s" % (file_root, next(count), file_ext)
        return name


    def _put_file(self, name, content):

        if self.encrypt_func:
            content = self.encrypt_func(content)

        policy = rs.PutPolicy(self.bucket)
        uptoken = policy.token()
        ret, err = io.put(uptoken, name, content)
        if err is not None:
            raise IOError("QiniuStorageError: %s", err)

    def delete(self, name):
        name = self._clean_name(name)
        rsp, err = rs.Client().delete(self.bucket, name)
        if err:
            raise IOError('QiniuStorageError %s', err)

    def exists(self, name):
        name = self._clean_name(name)
        rsp, err = rs.Client().stat(self.bucket, name)
        return rsp is not None

    def size(self, name):
        name = self._clean_name(name)
        rsp, err = rs.Client().stat(self.bucket, name)
        if rsp:
            return rsp['fsize']
        return 0

    def url(self, name):
        name = self._clean_name(name)
        base_url = rs.make_base_url(self.bucket_host, name)
        if self.is_private:
            policy = rs.GetPolicy()
            return policy.make_request(base_url)
        else:
            return base_url


class QiniuFile(File):
    """七牛文件，仅作读取文件用。
    """
    def __init__(self, name, storage, mode):
        self._name = name
        self._storage = storage
        self._mode = mode
        self._is_dirty = False
        self.file = StringIO()
        self.start_range = 0

    @property
    def size(self):
        if not hasattr(self, '_size'):
            self._size = self._storage.size(self._name)
        return self._size

    def write(self, content):
        if 'w' not in self._mode:
            raise AttributeError("File was opened for read-only access.")
        self.file = StringIO(content)
        self._is_dirty = True

    def close(self):
        if self._is_dirty:
            self._storage._put_file(self._name, self.file.getvalue())
        self.file.close()

    def read(self, num_bytes=None):
        """读取文件，num_bytes指定读取的字节数。
        如果指定num_bytes且文件原来是加密的，则每次读取到的数据不会被解密，
        而需要读取完毕后手动解密。
        """
        if num_bytes is None:
            args = []
            self.start_range = 0
        else:
            args = [self.start_range, self.start_range + num_bytes - 1]
        data, content_range = self._storage._read(self._name, *args)
        if content_range is not None:
            current_range, size = content_range.split(' ', 1)[1].split('/', 1)
            start_range, end_range = current_range.split('-', 1)
            self._size, self.start_range = int(size), int(end_range) + 1

        self.file = StringIO(data)
        return self.file.getvalue()
