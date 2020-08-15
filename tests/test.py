import unittest,sys 
sys.path.insert(1, 'c:/users/kagimub/desktop/IBRD Project')
from main import ETL


class TestFactorial(unittest.TestCase):
    """
    Our basic test class
    """

    def test_fact(self):
        """
        The actual test.
        Any method which starts with ``test_`` will considered as a test case.
        """
        etl = ETL()
        path = etl.DownloadingEmailAttachment('Bernard Kagimu', 'HELLO TEST MAIL')
        self.assertIsInstance(path, str)

        


if __name__ == '__main__':
    unittest.main()