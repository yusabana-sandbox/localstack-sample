import unittest
import decimal
from dynamodb import Dynamodb

class TestDynamodbCreate(unittest.TestCase):
    def setUp(self):
        self.db = Dynamodb('Movies')

    def tearDown(self):
        self.db.drop_table()

    def test_create_table(self):
        self.assertEqual(self.db.table_status(), 'ACTIVE')

class TestDynamodbOps(unittest.TestCase):
    def setUp(self):
        self.db = Dynamodb('Movies')
        self.db.put_item(
            {
               'year': 2017,
               'title': 'The Movie',
               'info': { 'rating': decimal.Decimal(0) }
            }
        )

    def tearDown(self):
        self.db.drop_table()

    def test_valid_get_item(self):
        item = self.db.get_item(2017, 'The Movie')
        assert item is not None

    def test_invalid_get_item(self):
        item = self.db.get_item(2017, 'Invalid title')
        assert item is None

if __name__ == "__main__":
    unittest.main()
