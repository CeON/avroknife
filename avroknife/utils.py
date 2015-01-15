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

import json
import base64
import os
import errno
from collections import OrderedDict

def to_byte_string(obj):
    """
    Converts an object into to a Python byte string
    """
    if(isinstance(obj, unicode)):
        return obj.encode("utf-8")
    else:
        return str(obj)

## Previously this class was created using the "namedtuple" function, but
## it didn't work properly. The JSON library that turns Python object
## into JSON string interpreted the binary string as a normal UTF8 string.
class EncapsulatedString:
    def __init__(self, string):
        self.string = string

def encapsulate_strings(python_object):
    """Converts every Python string (_not_ Unicode) to an EncapsulatedString

    Avro DataFileReader yields binary data as a Python string. To be able to jsonify it
    with a custom function we need to convert all strings to an object not understood by
    the JSON encoder. This is a kind of a hack, but on the same time the only solution found
    """
    if isinstance(python_object, str):
        return EncapsulatedString(python_object)
    if isinstance(python_object, list):
        return [encapsulate_strings(e) for e in python_object]
    if isinstance(python_object, OrderedDict):
        new_dict = OrderedDict()
        for (k, v) in python_object.iteritems():
            new_dict[k] = encapsulate_strings(v)
        return new_dict
    if isinstance(python_object, dict):
        return {k: encapsulate_strings(v) for (k, v) in python_object.iteritems()}
    return python_object

class _AvroJSONEncoder(json.JSONEncoder):
    """Custom JSON dumping class for correct handling of Avro bytes fields"""

    def default(self, python_object):
        """Serializes binary data which is otherwise impossible to dump as JSON"""
        if isinstance(python_object, EncapsulatedString):
            return base64.b64encode(python_object.string) 
        else:
            return super(_AvroJSONEncoder, self).default(python_object)

def dict_to_json(python_dict, pretty_print=False):
    """Dumps a Python dictionary to JSON format

    Args:
        python_dict: a Python dictionary containing Avro data_store
    Returns:
        A string being a valid JSON with binary data encoded with Base64
    """
    if pretty_print:
        return _AvroJSONEncoder(indent=4).encode(python_dict)
    else:
        return _AvroJSONEncoder().encode(python_dict)

class FileAlreadyExistsException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
