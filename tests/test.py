import unittest,sys 
sys.path.insert(1, 'c:/users/kagimub/desktop/IBRD Project')
from main import ETL


class ETLTest(unittest.TestCase):
    """ ETL Test Cases"""


    def DownloadingEmailAttachmentTest(self):
        """ Assert that functions returns a string File Path"""

        etl = ETL()
        path = etl.DownloadingEmailAttachment('Bernard Kagimu', 'HELLO TEST MAIL')
        self.assertIsInstance(path, str)

        


if __name__ == '__main__':
    unittest.main()