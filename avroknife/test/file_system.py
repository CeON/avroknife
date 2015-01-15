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

import os
import uuid
import tempfile
import shutil

from avroknife.file_system import import_hdfs_lib

class FileSystem:
    """File system abstraction"""

    def create_temporary_dir(self, dir=None):
        """
        Args:
            dir: If dir is specified, the file will be created in that directory; 
                otherwise, a default directory is used.

        Returns:
            path to the directory
        """
        raise NotImplementedError

    def delete_dir(self, path):
        raise NotImplementedError

    def create_dir(self, path):
        raise NotImplementedError

    def copy_from_local_dir(self, src_dir, dst_dir):
        """Copy directory from a local file system
        
        Args:
            dst_dir: the destination directory; it mustn't already exist.
        """
        raise NotImplementedError

    def copy_to_local_dir(self, src_dir, dst_dir):
        """Copy directory to a local file system
        
        Args:
            dst_dir: the destination directory; it mustn't already exist.
        """
        raise NotImplementedError

    def join_path(self, path_elements):
        raise NotImplementedError

class LocalFS(FileSystem):
    """Local file system"""

    def create_temporary_dir(self, dir=None):
        return tempfile.mkdtemp(dir=dir)

    def delete_dir(self, path):
        shutil.rmtree(path)

    def create_dir(self, path):
        os.makedirs(path)

    def copy_from_local_dir(self, src_dir, dst_dir):
        shutil.copytree(src_dir, dst_dir) 

    def copy_to_local_dir(self, src_dir, dst_dir):
        self.copy_from_local_dir(src_dir, dst_dir)

    def join_path(self, path_elements):
        return os.path.join(*path_elements)

class HDFS(FileSystem):
    """Hadoop Distributed File System"""

    __default_tmp_dir = ['.tmp-avroknife']

    def __init__(self):
        (hdfs, hdfspath) = import_hdfs_lib()
        self.hdfs = hdfs
        self.hdfspath = hdfspath

    def create_temporary_dir(self, dir=None):
        file_exists = True
        path = None
        while file_exists:
            id_ = str(uuid.uuid4())
            if dir is not None:
                path = self.join_path([dir, id_])
            else:
                path = self.join_path(self.__default_tmp_dir + [id_])
            file_exists = self.hdfspath.exists(path)
        self.create_dir(path)
        return path

    def delete_dir(self, path):
        self.hdfs.rmr(path)

    def create_dir(self, path):
        self.hdfs.mkdir(path)

    def copy_from_local_dir(self, src_dir, dst_dir):
        self.hdfs.put(src_dir, dst_dir)

    def copy_to_local_dir(self, src_dir, dst_dir):
        self.hdfs.get(src_dir, dst_dir)

    def join_path(self, path_elements):
        return '/'.join(path_elements)
