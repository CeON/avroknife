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

import json

from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from avroknife.error import error
from avroknife.utils import encapsulate_strings, dict_to_json, to_byte_string

def to_json(data_store, record_selector, printer, pretty=False):
    """Converts selected records to JSON.
    
    Args:
        data_store: a DataStore object
        record_selector: a RecordSelector object that defines which records
            should be processed.
        printer: a Printer object that is used to print the JSON.
        pretty: True if the output should be a valid, pretty-printed JSON.
    """
    first_record = True
    if pretty:
        printer.print("[", end="") 
    for record in record_selector.get_records(data_store):
        try:
            if first_record:
                first_record = False
            else:
                if pretty:
                    printer.print(",")
                else:
                    printer.print("")
            content = encapsulate_strings(record.content)
            printer.print(dict_to_json(content, pretty), end="")
        except Exception:
            error("while processing record with index {}".format(record.index))
            raise
    if pretty:
        printer.print("]")
    else:
        ## The case when no records were present
        if not first_record:
            printer.print("")

def extract(data_store, record_selector, field_name, output_dir_path=None):
    """Extract specified field from selected records

    Args:
        data_store: a DataStore object
        record_selector: a RecordSelector object. It defines which records
            should be processed.
        field_name: name of the field to be extracted. Can contain nested names,
            e.g. 'field1.field2' .
        output_dir_path: a FileSystemPath object. This is where the extracted
            fields will be saved. If this is not given, the extracted fields
            are printed to stdout.
    """
    if output_dir_path is not None:
        output_dir_path.make_dirs()
    for record in record_selector.get_records(data_store):
        datum = record.content
        if datum: #check if there is any data to be written
            parts = field_name.split('.')
            for part in parts:
                datum = datum[part]
            if output_dir_path is not None:
                try:
                    with output_dir_path.append(str(record.index)).open('w') \
                            as output:
                        output.write(to_byte_string(datum))
                except Exception:
                    error("while processing record with index {}"\
                        .format(record.index))
                    raise
            else:
                print(to_byte_string(datum))

def copy(data_store, record_selector, output_dir_path):
    """Dump selected records from the data store to another Avro file
    
    Args:
        data_store: a DataStore object
        record_selector: a RecordSelector object. It defines which records
            should be processed.
        output_dir_path: a FileSystemPath object. This is where the dump will
            be saved.
    """
    output_dir_path.make_dirs()
    with output_dir_path.append("content.avro").open("w") as output:
        with DataFileWriter(output, DatumWriter(), data_store.get_schema()) \
                as writer:
            records = record_selector.get_records(data_store)
            for record in records:
                writer.append(record.content)

def get_schema(data_store):
    """Get data store schema
    
    Args:
        data_store: a DataStore object
    Returns:
        JSON schema
    """
    return dict_to_json(json.loads(str(data_store.get_schema())), True)

def count(data_store, record_selector):
    """Get the number of selected records in the data store
    
    Args:
        data_store: a DataStore object
        record_selector: a RecordSelector object. It defines which records
            should be processed.
    Returns:
        number
    """
    n = 0
    for _ in record_selector.get_records(data_store):
        n = n+1
    return n