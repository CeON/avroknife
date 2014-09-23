Refactoring of internal code
============================
The implementation of the command-line interface is a little bit messy - the DRY principle is violated. Maybe it would be a good idea to use the `ArgumentParser.add_subparsers()`?
