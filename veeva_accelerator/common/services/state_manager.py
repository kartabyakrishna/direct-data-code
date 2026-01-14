import boto3
import os
from botocore.exceptions import ClientError
from common.utilities import log_message

class StateManager:
    def __init__(self, parameter_name: str, region_name: str = 'us-east-1'):
        self.parameter_name = parameter_name
        self.region_name = region_name
        # Initialize SSM client
        # In AWS Batch (Fargate), credentials are auto-handled by the task role
        self.ssm = boto3.client('ssm', region_name=self.region_name)

    def get_last_processed_time(self) -> str | None:
        """
        Retrieves the last processed timestamp from SSM Parameter Store.
        Returns None if parameter doesn't exist.
        """
        try:
            log_message('Info', f"Retrieving state from parameter: {self.parameter_name}")
            response = self.ssm.get_parameter(Name=self.parameter_name)
            value = response['Parameter']['Value']
            log_message('Info', f"Found last processed time: {value}")
            return value
        except self.ssm.exceptions.ParameterNotFound:
            log_message('Info', f"Parameter {self.parameter_name} not found. Starting fresh.")
            return None
        except ClientError as e:
            log_message('Error', f"Failed to retrieve parameter {self.parameter_name}", exception=e)
            raise e

    def set_last_processed_time(self, timestamp: str):
        """
        Updates the last processed timestamp in SSM Parameter Store.
        """
        try:
            log_message('Info', f"Updating state {self.parameter_name} to {timestamp}")
            self.ssm.put_parameter(
                Name=self.parameter_name,
                Value=timestamp,
                Type='String',
                Overwrite=True
                Type='String',
                Overwrite=True
            )
        except ClientError as e:
            log_message('Error', f"Failed to update parameter {self.parameter_name}", exception=e)
            raise e

class DynamoDBStateManager:
    def __init__(self, table_name: str, region_name: str = 'us-east-1'):
        self.table_name = table_name
        self.region_name = region_name
        self.dynamodb = boto3.resource('dynamodb', region_name=self.region_name)
        self.table = self.dynamodb.Table(self.table_name)

    def get_last_processed_time(self, process_id: str) -> str | None:
        """
        Retrieves the last processed timestamp/ID for a specific process (e.g., 'VeevaRedshift').
        """
        try:
            log_message('Info', f"Retrieving state from table: {self.table_name} for Key: {process_id}")
            response = self.table.get_item(Key={'process_id': process_id})
            if 'Item' in response:
                value = response['Item'].get('watermark')
                log_message('Info', f"Found watermark: {value}")
                return value
            else:
                log_message('Info', f"No watermark found for {process_id}. Starting fresh.")
                return None
        except ClientError as e:
            log_message('Error', f"Failed to retrieve state from DynamoDB", exception=e)
            raise e

    def set_last_processed_time(self, process_id: str, timestamp: str):
        """
        Updates the watermark in DynamoDB.
        """
        try:
            log_message('Info', f"Updating watermark for {process_id} to {timestamp}")
            self.table.put_item(
                Item={
                    'process_id': process_id,
                    'watermark': timestamp,
                    'updated_at': datetime.utcnow().isoformat()
                }
            )
        except ClientError as e:
            log_message('Error', f"Failed to update watermark in DynamoDB", exception=e)
            raise e

