import json
import logging
import os
from io import StringIO

import boto3
import pandas as pd

import transaction_optimization

AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
AWS_ENDPOINT_URL = os.getenv('AWS_ENDPOINT_URL')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
WORKER_QUEUE_URL = os.getenv('WORKER_QUEUE_URL')
DEBTS_BUCKET_NAME = os.getenv('DEBTS_BUCKET_NAME')

sqs = boto3.client('sqs', region_name=AWS_DEFAULT_REGION, endpoint_url=AWS_ENDPOINT_URL,
                   aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3 = boto3.client('s3', region_name=AWS_DEFAULT_REGION, endpoint_url=AWS_ENDPOINT_URL,
                  aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
logger = logging.getLogger(__name__)


def save_to_s3(data, debts_id):
    try:
        filename = f"{debts_id}_results"
        s3.put_object(Bucket=DEBTS_BUCKET_NAME, Key=filename, Body=data)
    except Exception as e:
        logger.error(f"Failed to save data to S3 for debts_id: {debts_id}. Error: {e}")


def read_transaction_data_from_s3(debts_id):
    filename = f"{debts_id}"
    try:
        response = s3.get_object(Bucket=DEBTS_BUCKET_NAME, Key=filename)
        data_str = response['Body'].read().decode('utf-8')
        data = pd.read_csv(StringIO(data_str), header=None, names=['Creditor', 'Debtor', 'Amount'])
        return data
    except Exception as e:
        logger.error(f"Failed to fetch data from S3 for debts_id: {debts_id}. Error: {e}")
        return None


def transations_optimization(data):
    neighbours = transaction_optimization.load_neighbours_to_graph(data)
    graphs = transaction_optimization.find_graphs(neighbours)
    balance = data.groupby('Creditor')['Amount'].sum().subtract(data.groupby('Debtor')['Amount'].sum(), fill_value=0)
    debtors_list, creditors_list = transaction_optimization.split_balances(graphs, balance)
    return transaction_optimization.calculate_transfers(debtors_list, creditors_list)


def main():
    try:
        response = sqs.receive_message(QueueUrl=WORKER_QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=20)
        if 'Messages' in response:
            message = response['Messages'][0]
            debts_id = json.loads(message['Body'])["debts_id"]
            logger.info(f"Statred debts_id: {str(debts_id)} processing")
            try:
                data = read_transaction_data_from_s3(debts_id)
                optimized_transations = transations_optimization(data)
                optimized_transations_csv = '\n'.join(
                    ','.join(map(str, transaction)) for transaction in optimized_transations)
                save_to_s3(optimized_transations_csv, debts_id)
                logger.info(f"Finished processing debts_id: {str(debts_id)}")
            except Exception as e:
                logger.error(f"Processing failed for debts_id: {debts_id}. Error: {e}")
            finally:
                sqs.delete_message(QueueUrl=WORKER_QUEUE_URL, ReceiptHandle=message['ReceiptHandle'])
    except Exception as e:
        logger.error(f"Error in message handling: {e}")


if __name__ == "__main__":
    logger.info("Worker started")
    while True:
        main()
