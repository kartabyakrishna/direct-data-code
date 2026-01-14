import boto3
import json
import os
from botocore.exceptions import ClientError
from common.utilities import log_message

class FailureHandler:
    def __init__(self, sns_topic_arn: str, eventbridge_rule_name: str, region_name: str = None):
        self.sns_topic_arn = sns_topic_arn
        self.eventbridge_rule_name = eventbridge_rule_name
        self.region_name = region_name or os.environ.get('AWS_REGION')
        self.sns = boto3.client('sns', region_name=self.region_name)
        self.events = boto3.client('events', region_name=self.region_name)

    def publish_alert(self, error_message: str, context: dict = None):
        """
        Publishes a high-priority alert to SNS.
        """
        try:
            message = {
                "error": error_message,
                "context": context or {},
                "status": "CRITICAL_FAILURE",
                "action_taken": "Scheduler DISABLED. Manual intervention required."
            }
            log_message('Info', f"Publishing alert to SNS: {self.sns_topic_arn}")
            self.sns.publish(
                TopicArn=self.sns_topic_arn,
                Message=json.dumps(message, indent=2),
                Subject="[Veeva-Redshift] Critical Pipeline Failure"
            )
        except ClientError as e:
            log_message('Error', "Failed to publish SNS alert", exception=e)

    def disable_scheduler(self):
        """
        Disables the EventBridge rule to prevent future runs.
        """
        try:
            log_message('Info', f"Disabling EventBridge Rule: {self.eventbridge_rule_name}")
            self.events.disable_rule(Name=self.eventbridge_rule_name)
        except ClientError as e:
            log_message('Error', f"Failed to disable EventBridge rule {self.eventbridge_rule_name}", exception=e)

    def enable_scheduler(self):
        """
        Enables the EventBridge rule (Resume Normal Operations).
        """
        try:
            log_message('Info', f"Enabling EventBridge Rule: {self.eventbridge_rule_name}")
            self.events.enable_rule(Name=self.eventbridge_rule_name)
        except ClientError as e:
            log_message('Error', f"Failed to enable EventBridge rule {self.eventbridge_rule_name}", exception=e)


    def handle_critical_failure(self, exception: Exception, context: dict = None):
        """
        Orchestrates the response to a critical failure:
        1. Disable Scheduler
        2. Publish Alert
        """
        error_msg = str(exception)
        self.disable_scheduler()
        self.publish_alert(error_msg, context)
