import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
os.environ['DEBTS_BUCKET_NAME'] = 'test'

import unittest
from moto import mock_aws
import boto3
import pandas as pd
from io import StringIO
from src.main import read_transaction_data_from_s3, save_to_s3, transations_optimization

AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
DEBTS_BUCKET_NAME = os.getenv('DEBTS_BUCKET_NAME')


class TestS3Operations(unittest.TestCase):

    @mock_aws
    def test_read_transaction_data_from_s3(self):
        s3 = boto3.client('s3', region_name=AWS_DEFAULT_REGION)
        s3.create_bucket(Bucket=DEBTS_BUCKET_NAME)
        debts_id = "test"
        filename = f"{debts_id}"
        content = "a,b,1\nb,a,1\naa,bb,3\n"
        s3.put_object(Bucket=DEBTS_BUCKET_NAME, Key=filename, Body=content)
        data = read_transaction_data_from_s3(debts_id)
        expected_data = pd.read_csv(StringIO(content), header=None, names=['Creditor', 'Debtor', 'Amount'])
        pd.testing.assert_frame_equal(data, expected_data)

    @mock_aws
    def test_read_empty_file_from_s3(self):
        s3 = boto3.client('s3', region_name=AWS_DEFAULT_REGION)
        s3.create_bucket(Bucket=DEBTS_BUCKET_NAME)
        debts_id = "empty_file"
        filename = f"{debts_id}"
        content = ""
        s3.put_object(Bucket=DEBTS_BUCKET_NAME, Key=filename, Body=content)
        data = read_transaction_data_from_s3(debts_id)
        self.assertTrue(data.empty)

    @mock_aws
    def test_save_valid_data_to_s3(self):
        s3 = boto3.client('s3', region_name=AWS_DEFAULT_REGION)
        s3.create_bucket(Bucket=DEBTS_BUCKET_NAME)
        debts_id = "test"
        content = "a,b,100\nb,c,200\n"
        save_to_s3(content, debts_id)
        response = s3.get_object(Bucket=DEBTS_BUCKET_NAME, Key=f"{debts_id}_results")
        data = response['Body'].read().decode('utf-8')
        self.assertEqual(data, content)


class TestTransactionOptimization(unittest.TestCase):

    def test_data(self):
        data = [
            (
                pd.DataFrame({
                    'Creditor': ['Jacek', 'Dominik', 'Kasia', 'Michał'],
                    'Debtor': ['Dominik', 'Jacek', 'Dominik', 'Kamil'],
                    'Amount': [10, 5, 5, 13]
                }),
                [('Dominik', 'Jacek', 5), ('Dominik', 'Kasia', 5), ('Kamil', 'Michał', 13)]
            ),
            (
                pd.DataFrame({
                    'Creditor': ['Logan', 'Logan', 'Logan', 'James', 'Jessica', 'Mary', 'James', 'James', 'Mary',
                                 'Jessica',
                                 'Amanda', 'Logan', 'James', 'James', 'Logan', 'Mary', 'Logan', 'James', 'James',
                                 'Jessica'],
                    'Debtor': ['Jessica', 'Mary', 'Jessica', 'Jessica', 'James', 'James', 'Logan', 'Amanda', 'Amanda',
                               'Amanda',
                               'James', 'Amanda', 'Jessica', 'Logan', 'Mary', 'Logan', 'Amanda', 'Jessica', 'Logan',
                               'James'],
                    'Amount': [574, 45, 177, 42, 169, 651, 461, 493, 359, 400, 439, 605, 232, 742, 599, 827, 13, 538,
                               397, 952]
                }),
                [('Amanda', 'Mary', 1193), ('Logan', 'James', 414), ('Amanda', 'James', 238), ('Jessica', 'James', 42)]
            )
        ]
        return data

    def test_transactions_optimization(self):
        for data, expected in self.test_data():
            result = transations_optimization(data)
            print(result)
            self.assertCountEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
