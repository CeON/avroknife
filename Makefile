all: build-source-package

build-source-package: clean generate-rst-description-file
	./setup.py sdist

install-user: clean generate-rst-description-file
	./setup.py install --user

install: clean generate-rst-description-file
	sudo ./setup.py install

uninstall-user:
	pip uninstall avroknife

uninstall:
	sudo pip uninstall avroknife

## Tests run on the local file system as well as on HDFS.
## Note that in order to run them, you need to have HDFS accessible from this machine.
test:
	export PYTHONPATH=$PYTHONPATH:$(pwd); export AVROKNIFE_HDFS_TESTS=TRUE; nosetests -v

## Tests run on the local file system only
test-local:
	export PYTHONPATH=$PYTHONPATH:$(pwd); nosetests -v

check-package-metatada: generate-rst-description-file
	./setup.py check --restructuredtext

html-readme:
	mkdir -p tmp
	pandoc -N -t html -s --no-wrap -o tmp/README.html README.markdown

clean:
	rm -rf build dist avroknife.egg-info docs-api tmp

## PyPI requires the description of the package to be in the reStructuredText format.
## This is how we generate it from the Markdown README.
generate-rst-description-file:
	mkdir -p tmp
	pandoc --from=markdown --to=rst README.markdown -o tmp/README.rst

## Uplading to testpypi and pypi as defined below requires two profiles to be defined 
## in the `~/.pypirc` file: `test` and `pypi`. On my computer this looks like this:
## 
## [distutils]
## index-servers=
##     pypi
##     test
## 
## [test]
## repository = https://testpypi.python.org/pypi
## username = XXXX
## password = XXXX
## 
## [pypi]
## repository = https://pypi.python.org/pypi
## username = XXXX
## password = XXXX

## Note that in order to make the `bdisk_wheel` option work you need to have
## `wheel` Python package installed.

upload-to-testpypi-and-install: generate-rst-description-file
	./setup.py sdist bdist_wheel upload -r test
	pip install --user -i https://testpypi.python.org/pypi avroknife

upload-to-pypi: generate-rst-description-file
	./setup.py sdist bdist_egg bdist_wheel upload -r pypi
