# Copyright 2013-2015 University of Warsaw
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import os.path
import sys
import errno

def hdfs_filesystem_warning():
    return 'maybe you did not specify the right file system? '\
        'Remember that if you want to access files in the local file system, '\
        'you have to prefix the path with "{}" and '\
        'if you want to access HDFS, no prefix should be given.'\
        .format(FileSystemPathFactory.local_fs_path_prefix)

def import_hdfs_lib():
    """Imports required modules from pydoop package locally. 
    
    This way, if we don't want to use the functionality of accessing HDFS, 
    having pydoop installed in the system is not necessary.
    """
    try:
        import pydoop
    except ImportError:
        print('ERROR: It seems that you want to access HDFS. '\
              'The module "pydoop" needs to be installed in the system '\
              'in order to do it. See avroknife packages\'s README file or '\
              'package\'s description in the http://pypi.python.org/ repository '\
              '(section "Installation") for further details.\n'\
              'On the other hand, {}'\
            .format(hdfs_filesystem_warning()), file=sys.stderr)
        sys.exit(1)
    ## We're importing stuff from pydoop package only locally, 
    ## so if we don't want to use the functionality of the
    ## script of accessing HDFS, having pydoop installed in the system
    ## is not required.
    import pydoop.hdfs as hdfs
    import pydoop.hdfs.path as hdfspath
    return (hdfs, hdfspath)

class FileSystemPathFactory:
    local_fs_path_prefix = "local:"

    @staticmethod
    def create(path):
        if path.startswith(FileSystemPathFactory.local_fs_path_prefix):
            sub = path[len(FileSystemPathFactory.local_fs_path_prefix):]
            return LocalPath(sub)
        else:
            return HDFSPath(path)

class FileSystemPath:
    """File system and path abstraction"""
    
    def open(self, mode="r"):
        """Open file

        The returned file-like object must support "seek" operation, so 
        returning just a stream is not enough. 
        """
        raise NotImplementedError

    def ls(self):
        raise NotImplementedError

    def exists(self):
        raise NotImplementedError

    def is_dir(self):
        raise NotImplementedError

    def append(self, string):
        raise NotImplementedError

    def make_dirs(self):
        raise NotImplementedError

    def __lt__(self, other):
        return str(self) < str(other)

class LocalPath(FileSystemPath):
    def __init__(self, path):
        self.__path = path

    def open(self, mode="r"):
        return open(self.__path, mode=mode)
    
    def ls(self):
        return os.listdir(self.__path)

    def exists(self):
        return os.path.exists(self.__path)
    
    def is_dir(self):
        return os.path.isdir(self.__path)
    
    def append(self, string):
        return LocalPath(os.path.join(self.__path, string))

    def make_dirs(self):
        try:
            os.makedirs(self.__path)
        except OSError as ex:
            if ex.errno == errno.EEXIST and os.path.isdir(self.__path):
                pass

    def __str__(self):
        return self.__path

class HDFSPath(FileSystemPath):
    def __init__(self, path):
        (hdfs, hdfspath) = import_hdfs_lib()
        self.hdfs = hdfs
        self.hdfspath = hdfspath
        self.__path = path

    def open(self, mode="r"):
        return self.hdfs.open(self.__path, mode)
    
    def ls(self):
        files = []
        for absolute_path in self.hdfs.ls(self.__path):    
            files.append(os.path.basename(absolute_path))
        return files
    
    def exists(self):
        return self.hdfspath.exists(self.__path)
    
    def is_dir(self):
        return self.hdfspath.isdir(self.__path)
    
    def append(self, string):
        return HDFSPath("{}/{}".format(self.__path, string))

    def make_dirs(self):    
        self.hdfs.mkdir(self.__path)
            
    def __str__(self):
        return self.__path
