import unittest
from testtoposorter import TestTopoSorter
from testsqlservertemplates import TestSqlServerTemplates
from testsqlquerybuilder import TestSqlQueryBuilder
from testdbtable import TestDbTable
from testdbtableconnected import TestDbTableConnected


suite = unittest.TestSuite()
suite.addTest(unittest.makeSuite(TestTopoSorter))
suite.addTest(unittest.makeSuite(TestSqlServerTemplates))
suite.addTest(unittest.makeSuite(TestSqlQueryBuilder))
suite.addTest(unittest.makeSuite(TestDbTable))
suite.addTest(unittest.makeSuite(TestDbTableConnected))

runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
