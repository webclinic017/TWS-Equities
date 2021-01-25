import unittest
from datetime import datetime as dt
from tws_equities.tws_clients import HistoricalDataExtractor


"""
    HistoricalDataExtractor is a TWS Client that extracts data from the API
    and stored it in a dictionary using input ticker ID(s) as keys. The same
    dictionary can be by calling the "data" attribute of the TWS Client object.
    
    This module tries to test the behavior of this client by extracting
    relevant data from TWS API and validating the data against a number of
    conditions.
"""


class TestHistoricalDataExtractor(unittest.TestCase):

    # def __init__(self):
    #     pass

    @classmethod
    def setUpClass(cls):
        postive_test_tickers = [
                                    1301,
                                    1429,
                                    1860,
                                    2685,
                                    3393,
                                    5386,
                                    7203,
                                    8729,
                                ]

        date_format = r'%Y%m%d'
        end_date = dt.today().date().strftime(date_format)  # current date
        end_time = r'15:01:01'  # after market close
        cls.client = HistoricalDataExtractor(end_date=end_date, end_time=end_time)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_extractor(self):
        test_tickers = [1301, 1302, 1303, 1304, 1305]
        self.client.extract_historical_data(test_tickers)


if __name__ == '__main__':
    unittest.main()
