# -*- coding: utf-8 -*-
""" Discovergy Data Analyzer
Usage:
   {cmd} <command> [<args>...]
   {cmd} -h | --help | --version

Commands:
   poll       Poll data from the Discovergy endpoint

Options:
   -h, --help

"""

import sys

from discovergy import __version__, poller

from docopt import docopt

from .config import read_config
from .utils import start_logging


def print_help() -> None:
    """Print the help and exit."""
    print("The sub command is unknown. Please try again.", end="\n\n")
    print(__doc__.format(cmd=sys.argv[0]), file=sys.stderr)
    sys.exit(1)


def main():
    """Parse arguments and dispatch to the submodule"""

    config = read_config()
    start_logging(config)

    dispatch = {
        "poll": poller.main,
    }

    arguments = docopt(
        __doc__.format(cmd=sys.argv[0]), version=__version__, options_first=True,
    )

    dispatch.get(arguments["<command>"], print_help)(config)


if __name__ == "__main__":
    main()
