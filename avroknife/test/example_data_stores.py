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
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

def create(standard_out_path, nested_out_path, binary_out_path):
    """Create example Avro data stores"""

    __create_standard(standard_out_path)
    __create_nested(nested_out_path)
    __create_binary(binary_out_path)

def __create_standard(out_path):
    os.makedirs(out_path)
    schema_path = os.path.join(os.path.dirname(__file__), 'data/user.avsc')
    schema = avro.schema.parse(open(schema_path).read())
  
    with DataFileWriter(open(os.path.join(out_path, 'part-m-00000.avro'), 'w'), 
            DatumWriter(), schema) as writer:
        writer.append({'position': 0, 'name': 'Alyssa', 'favorite_number': 256})
        writer.append({'position': 1, 'name': 'Ben', 'favorite_number': 4, 'favorite_color': 'red'})
    
    with DataFileWriter(open(os.path.join(out_path, 'part-m-00001.avro'), 'w'), 
            DatumWriter(), schema) as writer:
        writer.append({'position': 2, 'name': 'Alyssa2', 'favorite_number': 512})
        writer.append({'position': 3, 'name': 'Ben2', 'favorite_number': 8, 'favorite_color': 'blue', 'secret':b'0987654321'})
        writer.append({'position': 4, 'name': 'Ben3', 'favorite_number': 2, 'favorite_color': 'green', 'secret':b'12345abcd'})
    
    with DataFileWriter(open(os.path.join(out_path, 'part-m-00002.avro'), 'w'), 
            DatumWriter(), schema) as writer:
        pass
    
    with DataFileWriter(open(os.path.join(out_path, 'part-m-00003.avro'), 'w'), 
            DatumWriter(), schema) as writer:
        writer.append({'position': 5, 'name': 'Alyssa3', 'favorite_number': 16})
        writer.append({'position': 6, 'name': 'Mallet', 'favorite_color': 'blue', 'secret': b'asdfgf'})
        writer.append({'position': 7, 'name': 'Mikel', 'favorite_color': ''})
    
def __create_nested(out_path):
    os.makedirs(out_path)
    schema_path = os.path.join(os.path.dirname(__file__), 'data/nested.avsc')
    schema = avro.schema.parse(open(schema_path).read())
    with DataFileWriter(open(os.path.join(out_path, 'part-m-00004.avro'), 'w'), 
                    DatumWriter(), schema) as writer:
            writer.append({'sup': 1, 'sub':{'level2':2}})
            writer.append({'sup': 2, 'sub':{'level2':1}})

def __create_binary(out_path):
    os.makedirs(out_path)
    schema_path = os.path.join(os.path.dirname(__file__), 'data/binary.avsc')
    schema = avro.schema.parse(open(schema_path).read())
    with DataFileWriter(open(os.path.join(out_path, 'content.avro'), 'w'), 
                    DatumWriter(), schema) as writer:
            various_stuff_data = open(os.path.join(os.path.dirname(__file__), 
                            'data/binary_stuff/various_stuff.tar.gz')).read()
            writer.append({'description': 'various stuff', 
                           'packed_files': various_stuff_data})
            greetings_data = open(os.path.join(os.path.dirname(__file__), 
                            'data/binary_stuff/greetings.tar.gz')).read()
            writer.append({'description': 'greetings', 
                           'packed_files': greetings_data})
