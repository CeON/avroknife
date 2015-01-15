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

import unittest

from avroknife.test.tools import Tools

class ToolsTestCase(unittest.TestCase):
    @staticmethod
    def replace_according_to_map(key):
        my_map = {'simple': '/some/path/to/simple/file', 
            'path_with_space': '/some/path with/space',
            'name/with/slashes': '/some/other/path'}
        return my_map[key]

    def test_replace_prefix(self):
        text = 'interesting simple path:@in:simple and some other path that '\
            'follows it @in:path_with_space and some other path: '\
            '@in:name/with/slashes'
        expected = 'interesting simple path:/some/path/to/simple/file '\
            'and some other path that follows it /some/path with/space '\
            'and some other path: /some/other/path'
        actual = Tools.replace(text, '@in:', self.replace_according_to_map)
        self.assertEqual(expected, actual)
