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

class Printer:
    """Output printing abstraction"""
    def print(self, text, end="\n"):
        raise NotImplementedError 

    def __enter__(self):
        return self

    """Release the resources"""
    def close(self):
        raise NotImplementedError

    def __exit__(self, type, value, traceback):
        self.close()

class StdoutPrinter(Printer):
    """Prints to stdout"""
    def print(self, text, end="\n"):
        print(text, end=end)

    """Does nothing since this printer doesn't hold any resources"""
    def close(self):
        pass

class FilePrinter(Printer):
    def __init__(self, fs_path):
        self.__f = fs_path.open("w")

    """Prints to file"""
    def print(self, text, end="\n"):
        print(text, file=self.__f, end=end)

    def close(self):
        self.__f.close()
