import yaml
import logging
import datetime
from dateutil import tz
from google.cloud import bigquery
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)

# Load configuration
with open("config.yml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Set up BigQuery client
client = bigquery.Client(project=config["bigquery"]["project_id"])

# Set up Slack client
slack_client = WebClient(token=config["slack"]["token"])

from google.cloud import bigquery

class BigQueryClient:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)

    def check_last_modified_time(self, dataset_id, table_id):
        query = f"""
          SELECT
            max(last_modified_time) as last_modified
          FROM
            `{config["bigquery"]["project_id"]}.{dataset_id}.INFORMATION_SCHEMA.PARTITIONS`
          WHERE table_name = '{table_id}'
          GROUP BY table_name
        """
        query_job = self.client.query(query)
        result = query_job.result()
        result_list = list(result)
        if len(result_list) == 0:
            logging.warning(f"No result found for {dataset_id}.{table_id}")

            return None
        row = result_list[0]
        return row["last_modified"]

def target_date(date_pattern, today):
    if date_pattern == "yesterday":
        target_date = today - datetime.timedelta(days=1)
    elif date_pattern == "two_days_ago":
        target_date = today - datetime.timedelta(days=2)
    elif date_pattern == "month_start":
        target_date = today.replace(day=1)
    elif date_pattern == "year_start":
        target_date = today.replace(month=1, day=1)
    else:
        raise ValueError(f"Invalid date_pattern: {date_pattern}")
    return target_date

def send_slack_notification(message):
    logging.info(f"Sending Slack notification: {message}")
    try:
        response = slack_client.chat_postMessage(
            channel=config["slack"]["channel"],
            text=message
        )
    except SlackApiError as e:
        print(f"Error sending message: {e}")

def main():
    now = datetime.datetime.now(tz.UTC)
    for table in config["tables"]:
        dataset_id = table["dataset_id"]
        table_id = table["table_id"]
        logging.info(f"start checking last modified time for {dataset_id}.{table_id}")

        check_frequency = table["check_frequency"]
        check_time = datetime.datetime.strptime(table["check_time"], "%H:%M").time()

        if "date_suffix" in table:
            date_suffix = target_date(table["date_suffix"], now)
            table_id = f"{table_id}_{date_suffix.strftime('%Y%m%d')}"
        

        bq_client = BigQueryClient(config["bigquery"]["project_id"])
        last_modified = bq_client.check_last_modified_time(dataset_id, table_id)

        if last_modified is None:
            message = f"""
            :warning: {dataset_id}.{table_id} does not exist.
            """
            send_slack_notification(message)
            continue

        formatted_last_modified = last_modified.strftime("%Y-%m-%d %H:%M:%S")
        time_diff = now - last_modified

		# Before time threshold, table may not exist.
        if time_diff.total_seconds() / 3600 > check_frequency:
            message = f"""
            :warning:  {dataset_id}.{table_id} was last modified at {formatted_last_modified}. It hasn't been updated for more than {check_frequency} hours.
            """
            send_slack_notification(message)
        else:
            message = f"""
            :white_check_mark: {dataset_id}.{table_id} was last modified at {formatted_last_modified}. It has been updated within the last {check_frequency} hours.
            """
            send_slack_notification(message)

if __name__ == "__main__":
    main()
