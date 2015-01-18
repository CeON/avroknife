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

def extract(data_store, record_selector, value_field, 
    name_field=None, create_dirs=None, output_dir_path=None):
    """Extract specified field from selected records

    Args:
        data_store: a DataStore object
        record_selector: a RecordSelector object. It defines which records
            should be processed.
        value_field: name of the field to be extracted. 
            Can contain nested names, e.g. 'field1.field2' .
        name_field: value of this field is used as a name of the file 
            containing extracted data for given row. If this is not given,
            the name of the file is assumed to be the index of corresponding
            record.
        create_dirs: extracted fields corresponding to the same name 
            are to be placed in the same directory. 
            Name of directory is equal to the `name_field`; 
            names of the files inside are consecutive numbers.
        output_dir_path: a FileSystemPath object. This is where the extracted
            fields will be saved. If this is not given, the extracted fields
            are printed to stdout.
    """
    if output_dir_path is not None:
        output_dir_path.make_dirs()
    for record in record_selector.get_records(data_store):
        datum = record.content
        if datum: #check if there is any data to be written
            field_value = __get_value(datum, value_field)
            if output_dir_path is not None:
                try:
                    output_name = __get_output_name(record.index, datum, name_field)
                    output_path = output_dir_path.append(output_name)
                    file_path = None
                    if create_dirs:
                        file_path = __prepare_dir(output_path)
                    else:
                        file_path = __prepare_file(output_path, name_field) 
                    with file_path.open("w") as output:
                        output.write(to_byte_string(field_value))
                except Exception:
                    error("while processing record with index {}"\
                        .format(record.index))
                    raise
            else:
                print(to_byte_string(field_value))

def __prepare_dir(output_dir):
    if output_dir.exists():
        if not output_dir.is_dir():
            error("File with name '{}' already exists. "\
                "Unable to create a directory with the same name".\
                format(output_dir))
            raise Exception()
    else:
        output_dir.make_dirs()
    file_names = output_dir.ls()
    new_number = 0
    if len(file_names) > 0:
        file_numbers = [int(f) for f in file_names]
        new_number = max(file_numbers)+1
    new_file_name = str(new_number)
    return output_dir.append(new_file_name)

def __prepare_file(output_file, name_field):
    if output_file.exists():
        ## There is a race condition between checking 
        ## if the file exists and opening it for writing 
        ## but I don't see  an easy way to deal with it 
        ## in a way that would work for HDFS as well as 
        ## for local file system paths. Dealing with 
        ## this problem doesn't seem to be important anyway.
        error_string = "The file with name '{}' "\
            "already exists.".format(output_file) 
        if name_field is not None:
            error_string = error_string + " This is probably "\
                "because the selected values of given field "\
                "'{}' are not unique".format(name_field)
        error(error_string)
        raise Exception()
    return output_file

def __get_output_name(record_index, datum, name_field):
    file_name = str(record_index)
    if name_field is not None:
        file_name_raw = __get_value(datum, name_field)
        file_name = "null"
        if file_name_raw is not None:
            file_name_raw_str = str(file_name_raw).strip()
            if len(file_name_raw_str) > 0:
               file_name = file_name_raw_str 
    return file_name

def __get_value(datum, field_name):
    parts = field_name.split('.')
    try:
        for part in parts:
            datum = datum[part]
        return datum
    except KeyError:
        error("Field '{}' is not defined in the data".format(field_name))
        raise

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
