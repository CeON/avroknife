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

import unittest
import shutil
import tempfile
import os.path
import itertools
import filecmp
import distutils.dir_util

from avroknife.test.command_line_runner import CommandLineRunner, \
    CommandLineRunnerException
from avroknife.test import example_data_stores

class CommandLineTestCaseBase(unittest.TestCase):
    __source_data_dir = None
    _r = None
    __hdfs_tests_env_name = 'AVROKNIFE_HDFS_TESTS'
    __hdfs_tests = False
    
    @classmethod
    def setUpClass(cls):
        cls.__source_data_dir = tempfile.mkdtemp()
        distutils.dir_util.copy_tree(
            os.path.join(os.path.dirname(__file__), 'data/local_input_dir'),
            cls.__source_data_dir)
        standard_ds_path = os.path.join(cls.__source_data_dir, 'standard')
        nested_ds_path = os.path.join(cls.__source_data_dir, 'nested')
        binary_ds_path = os.path.join(cls.__source_data_dir, 'binary')
        example_data_stores.create(
            standard_ds_path, nested_ds_path, binary_ds_path)
        enforce_local = True
        env_name = CommandLineTestCaseBase.__hdfs_tests_env_name
        if os.getenv(env_name, 'FALSE')=='TRUE':
            cls.__hdfs_tests = True
            enforce_local = False
        cls._r = CommandLineRunner(
            './scripts/avroknife', cls.__source_data_dir, enforce_local)
    
    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.__source_data_dir)
        cls._r.close()
        
    @staticmethod
    def _read(file_path):
        with open(file_path, 'r') as f:
            content = f.read()
        return content
    
    @staticmethod
    def __bool_to_str(x):
        if x:
            return ' '
        else:
            return 'NOT'
    
    @staticmethod
    def __get_problem_str(in_local, out_local):
        return 'Test failed when inputs are{}local and outputs are{}local.'.\
            format(CommandLineTestCaseBase.__bool_to_str(in_local), 
                   CommandLineTestCaseBase.__bool_to_str(out_local))
    
    def _iterate(self, fun):
        in_local_values = [True]
        out_local_values = [True]
        if self.__hdfs_tests:
            in_local_values = [True, False]
            out_local_values = [True, False]            
        for in_local, out_local in itertools.product(
                        in_local_values, out_local_values):
            try:
                fun(in_local, out_local)
            except Exception:
                print(self.__get_problem_str(in_local, out_local))
                raise
    
    def _check_output(self, command_args, expected_stdout, in_local, out_local):
        ret = self._r.run(command_args, in_local, out_local)
        self.assertEqual(expected_stdout, ret.get_stdout())

    def _check_output_file_raw_content(self, command_args, expected_content, 
                                    output_file_name, in_local, out_local):
        ret = self._r.run(command_args, in_local, out_local)
        actual = self._read(ret.get_output_path(output_file_name))
        self.assertEqual(expected_content, actual)

    def _check_output_avro_file(self, command_args, expected_avro_json_content, 
                     output_file_name, in_local, out_local):
        ret = self._r.run(command_args, in_local, out_local)
        output_path = ret.get_output_path(output_file_name)
        actual = self._r.run_raw('tojson local:{}'.format(output_path))
        self.assertEqual(expected_avro_json_content, actual)
 
    @staticmethod
    def _get_expected_standard_schema():
        return """\
{
    "namespace": "avroknife.test.data", 
    "type": "record", 
    "name": "User", 
    "fields": [
        {
            "type": "int", 
            "name": "position"
        }, 
        {
            "type": "string", 
            "name": "name"
        }, 
        {
            "type": [
                "int", 
                "null"
            ], 
            "name": "favorite_number"
        }, 
        {
            "type": [
                "string", 
                "null"
            ], 
            "name": "favorite_color"
        }, 
        {
            "type": [
                "bytes", 
                "null"
            ], 
            "name": "secret"
        }
    ]
}
"""

    @staticmethod
    def _get_expected_standard_contents():
        return """\
{"position": 0, "name": "Alyssa", "favorite_number": 256, "favorite_color": null, "secret": null}
{"position": 1, "name": "Ben", "favorite_number": 4, "favorite_color": "red", "secret": null}
{"position": 2, "name": "Alyssa2", "favorite_number": 512, "favorite_color": null, "secret": null}
{"position": 3, "name": "Ben2", "favorite_number": 8, "favorite_color": "blue", "secret": "MDk4NzY1NDMyMQ=="}
{"position": 4, "name": "Ben3", "favorite_number": 2, "favorite_color": "green", "secret": "MTIzNDVhYmNk"}
{"position": 5, "name": "Alyssa3", "favorite_number": 16, "favorite_color": null, "secret": null}
{"position": 6, "name": "Mallet", "favorite_number": null, "favorite_color": "blue", "secret": "YXNkZmdm"}
{"position": 7, "name": "Mikel", "favorite_number": null, "favorite_color": "", "secret": null}
"""


class GetSchemaTestsCase(CommandLineTestCaseBase):
    def test_basic(self):
        self._iterate(self.subtest_basic)
    def subtest_basic(self, in_local, out_local):
        self._check_output('getschema @in:standard', 
            self._get_expected_standard_schema(), in_local, out_local)
     
    def test_output_file(self):
        self._iterate(self.subtest_output_file)
    def subtest_output_file(self, in_local, out_local):
        self._check_output_file_raw_content('getschema @in:standard --output @out:actual.txt',
            self._get_expected_standard_schema(), 'actual.txt',
            in_local, out_local)


class ToJSONTestsCase(CommandLineTestCaseBase):     
    def test_basic(self):
        self._iterate(self.subtest_basic)
    def subtest_basic(self, in_local, out_local):
        self._check_output('tojson @in:standard', 
                    self._get_expected_standard_contents(), in_local, out_local)
   
    def test_pretty(self):
        self._iterate(self.subtest_pretty)
    def subtest_pretty(self, in_local, out_local):
        self._check_output('tojson --pretty @in:standard', """\
[{
    "position": 0, 
    "name": "Alyssa", 
    "favorite_number": 256, 
    "favorite_color": null, 
    "secret": null
},
{
    "position": 1, 
    "name": "Ben", 
    "favorite_number": 4, 
    "favorite_color": "red", 
    "secret": null
},
{
    "position": 2, 
    "name": "Alyssa2", 
    "favorite_number": 512, 
    "favorite_color": null, 
    "secret": null
},
{
    "position": 3, 
    "name": "Ben2", 
    "favorite_number": 8, 
    "favorite_color": "blue", 
    "secret": "MDk4NzY1NDMyMQ=="
},
{
    "position": 4, 
    "name": "Ben3", 
    "favorite_number": 2, 
    "favorite_color": "green", 
    "secret": "MTIzNDVhYmNk"
},
{
    "position": 5, 
    "name": "Alyssa3", 
    "favorite_number": 16, 
    "favorite_color": null, 
    "secret": null
},
{
    "position": 6, 
    "name": "Mallet", 
    "favorite_number": null, 
    "favorite_color": "blue", 
    "secret": "YXNkZmdm"
},
{
    "position": 7, 
    "name": "Mikel", 
    "favorite_number": null, 
    "favorite_color": "", 
    "secret": null
}]
""", in_local, out_local)
    
    def test_range_stdout(self):
        self._iterate(self.subtest_range_stdout)
    def subtest_range_stdout(self, in_local, out_local):
        self._check_output('tojson @in:standard --index 3-4', """\
{"position": 3, "name": "Ben2", "favorite_number": 8, "favorite_color": "blue", "secret": "MDk4NzY1NDMyMQ=="}
{"position": 4, "name": "Ben3", "favorite_number": 2, "favorite_color": "green", "secret": "MTIzNDVhYmNk"}
""", in_local, out_local)
      
    def test_range_file_out(self):
        self._iterate(self.subtest_range_file_out)
    def subtest_range_file_out(self, in_local, out_local):
        self._check_output_file_raw_content('tojson @in:standard --index 3-4 --output @out:actual.txt', """\
{"position": 3, "name": "Ben2", "favorite_number": 8, "favorite_color": "blue", "secret": "MDk4NzY1NDMyMQ=="}
{"position": 4, "name": "Ben3", "favorite_number": 2, "favorite_color": "green", "secret": "MTIzNDVhYmNk"}
""", 'actual.txt', in_local, out_local)
     
    def test_range_without_beginning(self):
        self._iterate(self.subtest_range_without_beginning)
    def subtest_range_without_beginning(self, in_local, out_local):
        self._check_output('tojson @in:standard --index -4', """\
{"position": 0, "name": "Alyssa", "favorite_number": 256, "favorite_color": null, "secret": null}
{"position": 1, "name": "Ben", "favorite_number": 4, "favorite_color": "red", "secret": null}
{"position": 2, "name": "Alyssa2", "favorite_number": 512, "favorite_color": null, "secret": null}
{"position": 3, "name": "Ben2", "favorite_number": 8, "favorite_color": "blue", "secret": "MDk4NzY1NDMyMQ=="}
{"position": 4, "name": "Ben3", "favorite_number": 2, "favorite_color": "green", "secret": "MTIzNDVhYmNk"}
""", in_local, out_local)
    
    def test_range_without_end(self):
        self._iterate(self.subtest_range_without_end)
    def subtest_range_without_end(self, in_local, out_local):
        self._check_output('tojson @in:standard --index 4-', """\
{"position": 4, "name": "Ben3", "favorite_number": 2, "favorite_color": "green", "secret": "MTIzNDVhYmNk"}
{"position": 5, "name": "Alyssa3", "favorite_number": 16, "favorite_color": null, "secret": null}
{"position": 6, "name": "Mallet", "favorite_number": null, "favorite_color": "blue", "secret": "YXNkZmdm"}
{"position": 7, "name": "Mikel", "favorite_number": null, "favorite_color": "", "secret": null}
""", in_local, out_local)
     
    def test_single_index(self):
        self._iterate(self.subtest_single_index)
    def subtest_single_index(self, in_local, out_local):
        self._check_output('tojson @in:standard --index 4', """\
{"position": 4, "name": "Ben3", "favorite_number": 2, "favorite_color": "green", "secret": "MTIzNDVhYmNk"}
""", in_local, out_local)
     
    def test_edge_case_without_beginning(self):
        self._iterate(self.subtest_edge_case_without_beginning)
    def subtest_edge_case_without_beginning(self, in_local, out_local):
        self._check_output('tojson @in:standard --index -0', """\
{"position": 0, "name": "Alyssa", "favorite_number": 256, "favorite_color": null, "secret": null}
""", in_local, out_local)
   
    def test_edge_case_without_end(self):
        self._iterate(self.subtest_edge_case_without_end)
    def subtest_edge_case_without_end(self, in_local, out_local):
        self._check_output('tojson @in:standard --index 7-', """\
{"position": 7, "name": "Mikel", "favorite_number": null, "favorite_color": "", "secret": null}
""", in_local, out_local)


class CopyTestsCase(CommandLineTestCaseBase):  
    def test_basic(self):
        self._iterate(self.subtest_basic)
    def subtest_basic(self, in_local, out_local):
        self._check_output_avro_file('copy @in:standard --output @out:whole_copy', 
            self._get_expected_standard_contents(), 'whole_copy', in_local, out_local)


class ExtractTestsCase(CommandLineTestCaseBase):     
    def test_text_fields(self):
        self._iterate(self.subtest_text_fields)
    def subtest_text_fields(self, in_local, out_local):
        ret = self._r.run('extract @in:standard --index 2-3 --value_field name --output @out:extracted_name', 
                           in_local, out_local)
        output_path = ret.get_output_path('extracted_name')
        actual_2 = self._read(os.path.join(output_path, '2'))
        actual_3 = self._read(os.path.join(output_path, '3'))
        self.assertEqual('Alyssa2', actual_2)
        self.assertEqual('Ben2', actual_3)

    def test_create_dirs(self):
        self._iterate(self.subtest_create_dirs)
    def subtest_create_dirs(self, in_local, out_local):
        ret = self._r.run('extract @in:standard --index 2-3 --value_field name --create_dirs --output @out:extracted_name', 
                           in_local, out_local)
        output_path = ret.get_output_path('extracted_name')
        actual_2 = self._read(os.path.join(output_path, '2', '0'))
        self.assertEqual(1, len(os.listdir(os.path.join(output_path, '2'))))
        actual_3 = self._read(os.path.join(output_path, '3', '0'))
        self.assertEqual(1, len(os.listdir(os.path.join(output_path, '3'))))
        self.assertEqual('Alyssa2', actual_2)
        self.assertEqual('Ben2', actual_3)

    def test_name_field(self):
        self._iterate(self.subtest_name_field)
    def subtest_name_field(self, in_local, out_local):
        ret = self._r.run('extract @in:standard --index 2-3 --value_field name --name_field favorite_color --output @out:extracted_name', 
            in_local, out_local)
        output_path = ret.get_output_path('extracted_name')
        null = self._read(os.path.join(output_path, 'null'))
        blue = self._read(os.path.join(output_path, 'blue'))
        self.assertEqual('Alyssa2', null)
        self.assertEqual('Ben2', blue)

    def test_empty_name_field(self):
        self._iterate(self.subtest_empty_name_field)
    def subtest_empty_name_field(self, in_local, out_local):
        ret = self._r.run('extract @in:standard --index 7 --value_field name --name_field favorite_color --output @out:extracted_name', 
            in_local, out_local)
        output_path = ret.get_output_path('extracted_name')
        null = self._read(os.path.join(output_path, 'null'))
        self.assertEqual('Mikel', null)

    def test_create_dirs_name_field(self):
        self._iterate(self.subtest_create_dirs_name_field)
    def subtest_create_dirs_name_field(self, in_local, out_local):
        ret = self._r.run('extract @in:standard --index 2-3 --value_field name --name_field favorite_color --create_dirs --output @out:extracted_name', 
            in_local, out_local)
        output_path = ret.get_output_path('extracted_name')
        null = self._read(os.path.join(output_path, 'null', '0'))
        self.assertEqual(1, len(os.listdir(os.path.join(output_path, 'null'))))
        blue = self._read(os.path.join(output_path, 'blue', '0'))
        self.assertEqual(1, len(os.listdir(os.path.join(output_path, 'blue'))))
        self.assertEqual('Alyssa2', null)
        self.assertEqual('Ben2', blue)

    def test_name_field_with_repeated_names(self):
        self._iterate(self.subtest_name_field_with_repeated_names)
    def subtest_name_field_with_repeated_names(self, in_local, out_local):
        with self.assertRaises(CommandLineRunnerException):
            ## We suppress stderr because the program uses it to write
            ## some information about the error related to the fact that the 
            ## files to be created have the same name and this looks bad 
            ## in the unit tests report printed on console.
            self._r.run('extract @in:standard --index 3-7 --value_field name --name_field favorite_color --output @out:extracted_name', 
                in_local, out_local, discard_stderr=True)

    def test_create_dirs_name_field_with_repeated_names(self):
        self._iterate(self.subtest_create_dirs_name_field_with_repeated_names)
    def subtest_create_dirs_name_field_with_repeated_names(self, in_local, out_local):
        ret = self._r.run('extract @in:standard --value_field name --name_field favorite_color --create_dirs --output @out:extracted_name', 
            in_local, out_local)
        output_path = ret.get_output_path('extracted_name')
        null0 = self._read(os.path.join(output_path, 'null', '0'))
        red = self._read(os.path.join(output_path, 'red', '0'))
        null1 = self._read(os.path.join(output_path, 'null', '1'))
        blue0 = self._read(os.path.join(output_path, 'blue', '0'))
        green = self._read(os.path.join(output_path, 'green', '0'))
        null2 = self._read(os.path.join(output_path, 'null', '2'))
        blue1 = self._read(os.path.join(output_path, 'blue', '1'))
        empty = self._read(os.path.join(output_path, 'null', '3'))

        self.assertEqual(1, len(os.listdir(os.path.join(output_path, 'red'))))
        self.assertEqual(4, len(os.listdir(os.path.join(output_path, 'null'))))
        self.assertEqual(2, len(os.listdir(os.path.join(output_path, 'blue'))))
        self.assertEqual(1, len(os.listdir(os.path.join(output_path, 'green'))))

        self.assertEqual('Alyssa', null0)
        self.assertEqual('Ben', red)
        self.assertEqual('Alyssa2', null1)
        self.assertEqual('Ben2', blue0)
        self.assertEqual('Ben3', green)
        self.assertEqual('Alyssa3', null2)
        self.assertEqual('Mallet', blue1)
        self.assertEqual('Mikel', empty)

    @staticmethod
    def __are_files_identical(path0, path1):
        return filecmp.cmp(path0, path1, shallow=False)
    
    def test_binary_fields(self):
        self._iterate(self.subtest_binary_fields)
    def subtest_binary_fields(self, in_local, out_local):
        ret = self._r.run('extract @in:binary --value_field packed_files --output @out:extracted_packed_files', 
                           in_local, out_local)
        output_path = ret.get_output_path('extracted_packed_files')
        self.assertTrue(self.__are_files_identical(
            os.path.join(os.path.dirname(__file__), 'data/binary_stuff/various_stuff.tar.gz'), 
            os.path.join(output_path, '0')))
        self.assertTrue(self.__are_files_identical(
            os.path.join(os.path.dirname(__file__), 'data/binary_stuff/greetings.tar.gz'), 
            os.path.join(output_path, '1')))
    
    def test_text_field_stdout(self):
        self._iterate(self.subtest_text_field_stdout)
    def subtest_text_field_stdout(self, in_local, out_local):
        self._check_output('extract @in:standard --index 2 --value_field name', """\
Alyssa2
""", in_local, out_local)
    
    def test_nested_fields(self):
        self._iterate(self.subtest_nested_fields)
    def subtest_nested_fields(self, in_local, out_local):
        ret = self._r.run('extract @in:nested --value_field sub.level2 --output @out:nested', 
                           in_local, out_local)
        output_path = ret.get_output_path('nested')
        self.assertEqual('2', open(os.path.join(output_path, '0'), 'r').read())
        self.assertEqual('1', open(os.path.join(output_path, '1'), 'r').read())

    def test_nested_name_field(self):
        self._iterate(self.subtest_nested_name_field)
    def subtest_nested_name_field(self, in_local, out_local):
        ret = self._r.run('extract @in:nested --value_field sup --name_field sub.level2 --output @out:nested', 
                           in_local, out_local)
        output_path = ret.get_output_path('nested')
        self.assertEqual('1', open(os.path.join(output_path, '2'), 'r').read())
        self.assertEqual('2', open(os.path.join(output_path, '1'), 'r').read())

class SelectTestsCase(CommandLineTestCaseBase):     
    def test_number(self):
        self._iterate(self.subtest_number)
    def subtest_number(self, in_local, out_local):
        self._check_output('tojson @in:standard --select position=1', """\
{"position": 1, "name": "Ben", "favorite_number": 4, "favorite_color": "red", "secret": null}
""", in_local, out_local)
    
    def test_string(self):
        self._iterate(self.subtest_string)
    def subtest_string(self, in_local, out_local):
        self._check_output('tojson @in:standard --select name=Ben', """\
{"position": 1, "name": "Ben", "favorite_number": 4, "favorite_color": "red", "secret": null}
""", in_local, out_local)
    
    def test_empty_string(self):
        self._iterate(self.subtest_empty_string)
    def subtest_empty_string(self, in_local, out_local):
        self._check_output('tojson @in:standard --select favorite_color=""', """\
{"position": 7, "name": "Mikel", "favorite_number": null, "favorite_color": "", "secret": null}
""", in_local, out_local)
    
    def test_null(self):
        self._iterate(self.subtest_null)
    def subtest_null(self, in_local, out_local):
        self._check_output('tojson @in:standard --select favorite_color=null', """\
{"position": 0, "name": "Alyssa", "favorite_number": 256, "favorite_color": null, "secret": null}
{"position": 2, "name": "Alyssa2", "favorite_number": 512, "favorite_color": null, "secret": null}
{"position": 5, "name": "Alyssa3", "favorite_number": 16, "favorite_color": null, "secret": null}
""", in_local, out_local)
    
    def test_no_records(self):
        self._iterate(self.subtest_no_records)
    def subtest_no_records(self, in_local, out_local):
        self._check_output('tojson @in:standard --select name=Ben --index 2-', '', in_local, out_local)
    
    def test_no_records_pretty(self):
        self._iterate(self.subtest_no_records_pretty)
    def subtest_no_records_pretty(self, in_local, out_local):
        self._check_output('tojson --pretty @in:standard --select name=Ben --index 2-', '[]\n', in_local, out_local)
    
    def test_copy(self):
        self._iterate(self.subtest_copy)
    def subtest_copy(self, in_local, out_local):
        self._check_output_avro_file('copy @in:standard --select name=Ben --output @out:ben_copy', 
            """\
{"position": 1, "name": "Ben", "favorite_number": 4, "favorite_color": "red", "secret": null}
""", 'ben_copy', in_local, out_local)
    
    def test_extract(self):
        self._iterate(self.subtest_extract)
    def subtest_extract(self, in_local, out_local):
        ret = self._r.run('extract @in:standard --value_field name --select name=Ben --output @out:ben_name', 
                           in_local, out_local)
        output_path = ret.get_output_path('ben_name')
        actual_1 = self._read(os.path.join(output_path, '1'))
        self.assertEqual('Ben', actual_1)
   
    def test_nested_field(self):
        self._iterate(self.subtest_nested_field)
    def subtest_nested_field(self, in_local, out_local):
        self._check_output('tojson @in:nested --select sub.level2=2', """\
{"sup": 1, "sub": {"level2": 2}}
""", in_local, out_local)
 

class LimitTestsCase(CommandLineTestCaseBase):  
    def test_basic(self):
        self._iterate(self.subtest_basic)
    def subtest_basic(self, in_local, out_local):
        self._check_output('tojson @in:standard --limit 1', """\
{"position": 0, "name": "Alyssa", "favorite_number": 256, "favorite_color": null, "secret": null}
""", in_local, out_local)
 
    def test_select_no_output(self):
        self._iterate(self.subtest_select_no_output)
    def subtest_select_no_output(self, in_local, out_local):
        self._check_output('tojson @in:standard --select name=Ben --index 2-', '', in_local, out_local)
 
    def test_select(self):
        self._iterate(self.subtest_select)
    def subtest_select(self, in_local, out_local):
        self._check_output('tojson @in:standard --select favorite_color=blue', """\
{"position": 3, "name": "Ben2", "favorite_number": 8, "favorite_color": "blue", "secret": "MDk4NzY1NDMyMQ=="}
{"position": 6, "name": "Mallet", "favorite_number": null, "favorite_color": "blue", "secret": "YXNkZmdm"}
""", in_local, out_local)
 
    def test_select_with_limit(self):
        self._iterate(self.subtest_select_with_limit)
    def subtest_select_with_limit(self, in_local, out_local):
        self._check_output('tojson @in:standard --select favorite_color=blue --limit 1', """\
{"position": 3, "name": "Ben2", "favorite_number": 8, "favorite_color": "blue", "secret": "MDk4NzY1NDMyMQ=="}
""", in_local, out_local)


class SchemaProjectionTestsCase(CommandLineTestCaseBase): 
    def test_index(self):
        self._iterate(self.subtest_index)
    def subtest_index(self, in_local, out_local):
        self._check_output('tojson @in:standard --index -4 --schema @in:user_projection.avsc', """\
{"position": 0, "name": "Alyssa"}
{"position": 1, "name": "Ben"}
{"position": 2, "name": "Alyssa2"}
{"position": 3, "name": "Ben2"}
{"position": 4, "name": "Ben3"}
""", in_local, out_local)
 
    def test_copy(self):
        self._iterate(self.subtest_copy)
    def subtest_copy(self, in_local, out_local):
        self._check_output_avro_file('copy @in:standard --schema @in:user_projection.avsc --output @out:projected', 
            """\
{"position": 0, "name": "Alyssa"}
{"position": 1, "name": "Ben"}
{"position": 2, "name": "Alyssa2"}
{"position": 3, "name": "Ben2"}
{"position": 4, "name": "Ben3"}
{"position": 5, "name": "Alyssa3"}
{"position": 6, "name": "Mallet"}
{"position": 7, "name": "Mikel"}
""", 'projected', in_local, out_local)


class CountTestsCase(CommandLineTestCaseBase):     
    def test_basic(self):
        self._iterate(self.subtest_basic)
    def subtest_basic(self, in_local, out_local):
        self._check_output('count @in:standard', '8\n', in_local, out_local)
        
    def test_file_output(self):
        self._iterate(self.subtest_file_output)
    def subtest_file_output(self, in_local, out_local):
        self._check_output_file_raw_content('count @in:standard --output @out:actual.txt' , 
            '8\n', 'actual.txt', in_local, out_local)
    
    def test_select(self):
        self._iterate(self.subtest_select)
    def subtest_select(self, in_local, out_local):
        self._check_output('count @in:standard --select name=Ben', '1\n', in_local, out_local)
