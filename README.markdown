About
=====
This project provides a command-line utility for browsing and simple manipulation of Avro-based files.

[Apache Avro][] is a serialization format invented to be a language-independent way of communication between [Hadoop][] data processing tasks. Hadoop tasks produce output that, on an abstract level, can be regarded as a list of objects of the same type. In practice, when using Avro format, this list is represented in the file system as a directory containing many Avro-formatted files where each file has the same schema. We call this directory the **Avro data store**.

[Apache Avro]: http://avro.apache.org/
[Hadoop]: http://hadoop.apache.org/ 

`avroknife` allows for browsing and simple manipulation of Avro data stores. It was inspired by Avro library's own tool called `avro-tools` which is distributed with that library as a `*.jar` file. Apart from differences in particular functionalities, the main philosophical difference between these two is that `avroknife` operates on whole Avro data store while `avro-tools` operates on individual Avro files.

Features
========
- Accesses Avro data stores placed in the local file system as well as in the Hadoop Distributed File System (HDFS).
    - Note that in order to access HDFS, you need to have `pydoop` Python package installed.
- Provides the following execution modes (run `avroknife -h` for details):
    - prints out schema of data store,
    - dumps data store as JSON,
    - dumps selected records from data store as a new data store,
    - dumps a field from selected records to file system or to stdout,
    - prints number of records inside a data store.
- Allows for simple selection of the records to be accessed based on combination of the following constraints:
    - index range of the records,
    - limit set on number of returned records,
    - value of a field.

Usage examples
==============
Let's assume that we have an Avro data store available as `/user/$USER/example_data_store` directory in HDFS:

    $ hadoop fs -ls example_data_store
    Found 4 items
    -rw-r--r--   1 mafju supergroup        408 2014-09-18 11:36 /user/mafju/example_data_store/part-m-00000.avro
    -rw-r--r--   1 mafju supergroup        449 2014-09-18 11:36 /user/mafju/example_data_store/part-m-00001.avro
    -rw-r--r--   1 mafju supergroup        364 2014-09-18 11:36 /user/mafju/example_data_store/part-m-00002.avro
    -rw-r--r--   1 mafju supergroup        429 2014-09-18 11:36 /user/mafju/example_data_store/part-m-00003.avro

First, let's check the schema of the data store

    $ avroknife getschema example_data_store
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

Then, let's list all its records: 

    $ avroknife tojson example_data_store
    {"position": 0, "name": "Alyssa", "favorite_number": 256, "favorite_color": null, "secret": null}
    {"position": 1, "name": "Ben", "favorite_number": 4, "favorite_color": "red", "secret": null}
    {"position": 2, "name": "Alyssa2", "favorite_number": 512, "favorite_color": null, "secret": null}
    {"position": 3, "name": "Ben2", "favorite_number": 8, "favorite_color": "blue", "secret": "MDk4NzY1NDMyMQ=="}
    {"position": 4, "name": "Ben3", "favorite_number": 2, "favorite_color": "green", "secret": "MTIzNDVhYmNk"}
    {"position": 5, "name": "Alyssa3", "favorite_number": 16, "favorite_color": null, "secret": null}
    {"position": 6, "name": "Mallet", "favorite_number": null, "favorite_color": "blue", "secret": "YXNkZmdm"}
    {"position": 7, "name": "Mikel", "favorite_number": null, "favorite_color": "", "secret": null}

Now, let's select the records where the `favorite_color` attribute is equal `blue` and the index of the record is 5 of larger:

    $ avroknife tojson --select favorite_color="blue" --index 5- example_data_store
    {"position": 6, "name": "Mallet", "favorite_number": null, "favorite_color": "blue", "secret": "YXNkZmdm"}

Next, let's extract value of the `name` attribute for all records where the `favorite_color` attribute is equal `blue`:

    $ avroknife extract --value_field name --select favorite_color="blue" example_data_store
    Ben2
    Mallet

Note that if the data store was placed in the local file system, you would have to prefix its path with `local:`, e.g. 

    $ avroknife tojson local:example_data_store

That's it. Run `avroknife -h` to find out more about other modes and options of `avroknife`.

Installation
============
The project is available in the PyPI repository, so in oder to install it, you need to do

	sudo pip install avroknife

**If you want to access HDFS**, `pydoop` Python library needs to be installed in the system. You can follow the description on [Pydoop's documentation page](http://pydoop.sourceforge.net/docs/installation.html) in order to proceed with its installation. On Ubuntu 14.04, this boils down to the following steps:

- Install Hadoop. If you want to install it on a single node in a so-called pseudo-distributed mode, I recommend to use the Cloudera Hadoop distribution. This can be done by following Cloudera's [step-by-step guide](http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH4/latest/CDH4-Quick-Start/cdh4qs_topic_3_2.html). Apart from the `hadoop-0.20-conf-pseudo` package from the Cloudera repository that is mentioned in the guide, you also have to install `hadoop-client` package.
- Make sure that Java JDK is installed correctly. This can be done by executing the following steps.
    - Make sure that Java JDK is installed. This can be done by installing `openjdk-7-jdk` package, i.e., `sudo apt-get install openjdk-7-jdk`.
    - Make sure that the `JAVA_HOME` environment variable is set properly. This can be done by adding line `export JAVA_HOME="/usr/lib/jvm/default-java"` in `/etc/profile.d/my_env_vars.sh` file.
- Install the following Ubuntu packages: `libboost-python-dev`, `python-support`, `python-software-properties`, `libssl-dev`, i.e., `sudo apt-get install libboost-python-dev python-support python-software-properties libssl-dev`.
- Install the package through `pip`, i.e., `sudo -i pip install pydoop`.

Troubleshooting
===============
On my system (Ubuntu 14.04) with my installation of Hadoop (CDH 4.7.0), the following message was printed on stderr every time that I accessed HDFS:

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details

It turned out that among the jars loaded by the `pydoop` library, the `slf4j` jar was missing (the symbolic link to it was broken). In order to amend this problem I

- removed the broken symbolic link with `sudo rm /usr/lib/hadoop/client/slf4j-log4j12.jar`
- created a correct symbolic link with `sudo ln -s /usr/share/java/slf4j-log4j12.jar /usr/lib/hadoop/client/slf4j-log4j12.jar` (you need to have the `libslf4j-java` package installed in order to have the target jar file present).

History
=======
The initial version of `avroknife` was created in March 2013. The script has been used by the developers of the Information Inference Service in the [OpenAIREplus](http://cordis.europa.eu/project/rcn/100079_en.html) project.

License
=======
The code is licensed under Apache License, Version 2.0
