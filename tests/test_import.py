# -*- coding: utf-8 -*-

__author__ = "Frank Becker <fb@alien8.de>"
__copyright__ = "Frank Becker"
__license__ = "mit"

import importlib


def test_module_imports():
    """Test if all modules can be imported."""
    modules = ["api", "auth", "awattar", "cli", "config", "defaults", "poller", "utils", "weather"]
    for module in modules:
        try:
            importlib.import_module("discovergy." + module)
        except ImportError:
            assert False
        else:
            assert True
