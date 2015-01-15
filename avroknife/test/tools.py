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

import string

class Tools:
    @staticmethod
    def replace(text, prefix, replace_function):
        """Replace substring from text marked with a given prefix using
        replace_function. 
        
        Args:
            text: analyzed text
            prefix: prefix that defines a special substring to be replaced
            replace_function: function that accepts found substring and returns
                new value that should replace the substring
        """
        new = []
        while len(text) > 0:
            chunk_index = string.find(text, prefix)
            if chunk_index == -1:
                new.append(text)
                break
            key_start_index = chunk_index + len(prefix)
            key_end_index = string.find(text, ' ', key_start_index)
            if key_end_index == -1:
                key = text[key_start_index:]
            else:
                key = text[key_start_index:key_end_index]
            value = replace_function(key)
            new.append(text[:chunk_index])
            new.append(value)
            if key_end_index == -1:
                text = ''
            else:
                text = text[key_end_index:]
        return ''.join(new)
