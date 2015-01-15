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

import os.path
import subprocess

from avroknife.test.tools import Tools
from avroknife.test.file_system import HDFS, LocalFS

class RunResult:
    def __init__(self, stdout, input_dir, output_dir):
        self.__stdout = stdout
        self.__input_dir = input_dir
        self.__output_dir = output_dir

    def get_stdout(self):
        """Get produced stdout string"""
        return self.__stdout

    def get_input_path(self, name):
        """Get path to the input in the local file system"""
        return os.path.join(self.__input_dir, name)

    def get_output_path(self, name):
        """Get path to the output in the local file system"""
        return os.path.join(self.__output_dir, name)
    
    def __str__(self):
        return 'stdout: {}\n\ninput_dir={}\noutput_dir={}'.format(
            self.__stdout, self.__input_dir, self.__output_dir)

class CommandLineRunnerException:
    def __init__(self, exception):
        """
        Args:
            exception: instance of subprocess.CalledProcessError
        """
        self.returncode = exception.returncode
        self.cmd = exception.cmd
        self.output = exception.output

class CommandLineRunner:
    __input_file_prefix = '@in:' 
    __output_file_prefix = '@out:'
    __input_subdir = 'input'
    __outputs_subdir = 'outputs'
    __hdfs_path_prefix = ''
    __local_path_prefix = 'local:'

    def __init__(self, program_path, local_input_dir, enforce_local=False):
        """
        Args:
            program_path: path to the command line program to be executed
            local_input_dir: path to the directory containing input files
                that can be referenced through placeholders
            enforce_local: allow running the program only on local file system.
                HDFS is not accessed in this mode.
        """ 
        self.__program_path = program_path
        self.__enforce_local = enforce_local
        self.__local_fs = LocalFS()
        self.__local_tmp_dir = \
            self.__initialize_tmp_dirs(self.__local_fs, local_input_dir)
        if not self.__enforce_local:
            self.__hdfs = HDFS()
            self.__hdfs_tmp_dir = \
                self.__initialize_tmp_dirs(self.__hdfs, local_input_dir)
        self.__is_closed = False

    @staticmethod
    def __initialize_tmp_dirs(fs, local_input_dir):
        dir_ = fs.create_temporary_dir()
        fs.copy_from_local_dir(local_input_dir, 
            fs.join_path([dir_, CommandLineRunner.__input_subdir]))
        fs.create_dir(fs.join_path([dir_, CommandLineRunner.__outputs_subdir]))
        return dir_

    def run(self, args_string, is_input_local, is_output_local, 
            discard_stderr=False):
        """
        Execute program with replacing placeholders in arguments string

        Args:
            args_string: parameters of the program with file placeholders
            is_input_local: if True, the input placeholders will be replaced
                with paths in local file system. If False, they will be replaced
                with paths in HDFS.
            is_output_local: if True, the output placeholders will be replaced
                with path in local file system. If False, they will be replaced
                with paths in HDFS.
            ignore_stderr: if True, the standar output is discarded

        Returns:
            RunResult object

        Raises:
            CommandLineRunnerException: exception raised when executed process
                returns non-zero exit status.
        """
        if self.__is_closed:
            raise Exception('This object has been already closed')
        if self.__enforce_local:
            if not (is_input_local and is_output_local):
                raise Exception('is_input_local={}, is_output_local={}, while '\
                    'the enforce_local mode allows running the program only '\
                    'on local file system '.\
                    format(is_input_local, is_output_local)) 

        local_out_dir = self.__local_fs.create_temporary_dir(
            self.__local_fs.join_path([self.__local_tmp_dir, self.__outputs_subdir]))
        hdfs_out_dir = None
        if not is_output_local:
            hdfs_out_dir = self.__hdfs.create_temporary_dir(
                self.__hdfs.join_path([self.__hdfs_tmp_dir, self.__outputs_subdir]))

        args_replaced = self.__replace_args(args_string, 
            is_input_local, is_output_local, local_out_dir, hdfs_out_dir)

        stdout = self.run_raw(args_replaced, discard_stderr)
        
        if not is_output_local:
            ## We need to delete this directory because the copying operation
            ## requires that the destination directory doesn't already exist
            self.__local_fs.delete_dir(local_out_dir)
            self.__hdfs.copy_to_local_dir(hdfs_out_dir, local_out_dir)

        return RunResult(stdout, 
            os.path.join(self.__local_tmp_dir, self.__input_subdir), 
            local_out_dir)

    def run_raw(self, args_string, discard_stderr=False):
        """
        Execute program WITHOUT replacing placeholders in arguments string

        Args:
            args_string: parameters of the program
            discard_stderr: if True, the standard error is discarded

        Returns:
            stdout string
        """
        return self.__system(self.__program_path + ' ' + args_string, 
            discard_stderr)

    def __replace_args(self, args_string, is_input_local, is_output_local, 
            local_out_dir, hdfs_out_dir):
        text = args_string
        if is_input_local:
            text = self.__replace(text, self.__input_file_prefix,
                self.__local_path_prefix, self.__local_fs.join_path,
                [self.__local_tmp_dir, self.__input_subdir])
        else:
            text = self.__replace(text, self.__input_file_prefix,
                self.__hdfs_path_prefix, self.__hdfs.join_path,
                [self.__hdfs_tmp_dir, self.__input_subdir])
        if is_output_local:
            text = self.__replace(text, self.__output_file_prefix,
                self.__local_path_prefix, self.__local_fs.join_path,
                [local_out_dir])
        else:
            text = self.__replace(text, self.__output_file_prefix,
                self.__hdfs_path_prefix, self.__hdfs.join_path,
                [hdfs_out_dir])
        return text

    @staticmethod
    def __replace(text, placeholder_prefix, path_prefix, path_joiner_function, 
            dir_name_elements):
        """Replace placeholders with paths to files"""

        replaced = Tools.replace(text, placeholder_prefix, 
            lambda s: path_prefix + path_joiner_function(dir_name_elements + [s]))
        return replaced


    def close(self):
        """Do the cleanup"""
        if self.__is_closed:
            raise Exception('This object has been already closed')
        self.__is_closed = True
        self.__local_fs.delete_dir(self.__local_tmp_dir)
        if not self.__enforce_local:
            self.__hdfs.delete_dir(self.__hdfs_tmp_dir)

    @staticmethod
    def __system(command, discard_stderr):
        try:
            if discard_stderr:
                with open(os.devnull, 'w') as devnull:
                    return subprocess.check_output(
                            command, shell=True, stderr=devnull)
            else:
                return subprocess.check_output(command, shell=True)
        except subprocess.CalledProcessError as ex:
            raise CommandLineRunnerException(ex)
