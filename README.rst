==========
discovergy
==========


Fetch and analyze data from Discovergy_ smart meters, Awattar_ power provider, and `Open Weather Map <https://openweathermap.org>`_. Also, trigger events based on configurable conditions like
turn on the washing machine when power is cheap.


Description
===========

This is a hobby project to interact with the data of a Discovergy smart meter. It's sole purpose is to play with data, learn and to try out new things. Not all may make sense. You're welcome to use it, provide feedback, send patches...

Installation
============

It's a Python >= 3.8 package. Given that::

  # python --version
  Python 3.8.1

returns at least 3.8 install via::

  # pip install -e "git+https://github.com/a8/discovergy@master#egg=discovergy"

To run out of the cloned git repo::

  cd discovergy
  python -m discovergy

Getting Started
===============

First try if::

  discovergyctl -h

returns the help screen similar to::

    Discovergy Data Analyzer
    Usage:
       discovergyctl <command> [<args>...]
       discovergyctl -h | --help | --version

    Commands:
       poll       Poll data from the Discovergy endpoint

    Options:
       -h, --help

All there is for now is to start polling for data. If you run for the very first time you will be asked for config data. The config file is located in ``~/.config/discovergy/config.ini``.::

    discovergyctl poll

And watch out for errors... Feel free to modify the config file by hand.

TODO
====

* Detect devices
* Find the lowest price for a given consumption pattern in a given time range. E. g. charge your EV, run the washing machine, ...
* Visualization of data using `Jupyter Notebooks <https://jupyter.org>`_.

Note
====

This project has been set up using PyScaffold 3.2.3. For details and usage
information on PyScaffold see https://pyscaffold.org/.

.. _Awattar: https://www.awattar.de
.. _Discovergy: https://discovergy.com
.. _Open Weather Map: https://openweathermap.org
