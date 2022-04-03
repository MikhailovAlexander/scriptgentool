import unittest
from testtoposorter import TestTopoSorter
from testsqlservertemplates import TestSqlServerTemplates
from testsqlquerybuilder import TestSqlQueryBuilder
from testdbtable import TestDbTable
from testfilewriter import TestFileWriter
from testscriptgenerator import TestScriptGenerator


suite = unittest.TestSuite()
suite.addTest(unittest.makeSuite(TestTopoSorter))
suite.addTest(unittest.makeSuite(TestSqlServerTemplates))
suite.addTest(unittest.makeSuite(TestSqlQueryBuilder))
suite.addTest(unittest.makeSuite(TestDbTable))
suite.addTest(unittest.makeSuite(TestFileWriter))
suite.addTest(unittest.makeSuite(TestScriptGenerator))

runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
