import os
import datetime
from bigquery.client import BigQueryClient
from notifier.slack import SlackNotifier
from config.config import load_config

from dotenv import load_dotenv
load_dotenv()
SLACK_TOKEN = os.getenv("SLACK_TOKEN")

def main():
    # Load configuration from file
    config = load_config("config/config.yml")

    # Create BigQuery client instance
    bq_client = BigQueryClient(config["bigquery"]["project_id"])

    # Create Slack notifier instance
    slack_client = SlackNotifier(SLACK_TOKEN)

    # Get current time in UTC
    now = datetime.datetime.now(datetime.timezone.utc)

    # Check last modified time of each table and send notification if necessary
    for table in config["tables"]:
        dataset_id = table["dataset_id"]
        table_id = table["table_id"]
        check_frequency =  table["check_frequency"]
        check_time = table["check_time"]
        date_suffix = None
        if "date_suffix" in table:
            date_suffix = table["date_suffix"]

        message = bq_client.notify_table_updates(dataset_id, table_id, check_frequency, check_time, now, date_suffix=date_suffix)

        # Send notification if necessary
        if message is not None:
            slack_client.send_notification(config["slack"]["channel"], message)

if __name__ == "__main__":
    main()
