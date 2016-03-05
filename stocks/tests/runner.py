#! /usr/bin/env python
import unittest
import sys, os
sys.path.insert(0, os.path.split(os.path.split(os.path.dirname(os.path.abspath(__file__)))[0])[0])

testmodules = [
    'test_quotes',
    ]

suite = unittest.TestSuite()

for t in testmodules:
    try:
        # If the module defines a suite() function, call it to get the suite.
        mod = __import__(t, globals(), locals(), ['suite'])
        suitefn = getattr(mod, 'suite')
        suite.addTest(suitefn())
    except (ImportError, AttributeError):
        # else, just load all the test cases from the module.
        suite.addTest(unittest.defaultTestLoader.loadTestsFromName(t))

unittest.TextTestRunner().run(suite)