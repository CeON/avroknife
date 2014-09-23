# Copyright 2013-2014 University of Warsaw
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

class StdoutPrinter(Printer):
    """Prints to stdout"""
    def print(self, text, end="\n"):
        print(text, end=end)

class FilePrinter(Printer):
    """Prints to file"""
    def __init__(self, fs_path):
        self.__fs_path = fs_path
        self.__f = None
        self.__already_open = False

    def print(self, text, end="\n"):
        if not self.__already_open:
            self.__f = self.__fs_path.open("w")
            self.__already_open = True
        print(text, file=self.__f, end=end)