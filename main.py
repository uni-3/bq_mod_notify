import yaml
import logging
import datetime
from dateutil import tz
from google.cloud import bigquery
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from abc import ABC, abstractmethod
import os

from dotenv import load_dotenv
load_dotenv()
SLACK_TOKEN = os.getenv("SLACK_TOKEN")

logging.basicConfig(level=logging.INFO)

def load_config(file_path):
    """Load configuration from a file.

    Args:
        file_path (str): Path to the configuration file.

    Returns:
        dict: Parsed configuration data.

    Raises:
        FileNotFoundError: If the configuration file is not found.
        yaml.YAMLError: If there's an error parsing the configuration file.
    """
    try:
        with open(file_path, "r") as config_file:
            return yaml.safe_load(config_file)
    except FileNotFoundError:
        logging.error("Configuration file not found.")
        raise
    except yaml.YAMLError as exc:
        logging.error(f"Error in configuration file: {exc}")
        raise

config = load_config("config.yml")

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
    """Get the target date based on the date pattern.

    Args:
        date_pattern (str): Date pattern string (e.g., "yesterday", "month_start").
        today (datetime.datetime): The reference date.

    Returns:
        datetime.datetime: The target date.

    Raises:
        ValueError: If the date_pattern is invalid.
    """
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


class Notifier(ABC):

    @abstractmethod
    def send_notification(self, channel, message):
        pass

class SlackNotifier(Notifier):

    def __init__(self, token):
        self.client = WebClient(token=token)

    def send_notification(self, channel, message):
        try:
            response = self.client.chat_postMessage(
                channel=channel,
                text=message
            )
        except SlackApiError as e:
            print(f"Error sending message: {e}")



def prepare_notification_message(dataset_id, table_id, last_modified, check_frequency, now):
    """Prepare a notification message based on the table's last modification time and check frequency.

    Args:
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        last_modified (datetime.datetime): The last modification time of the table.
        check_frequency (float): The check frequency in hours.
        now (datetime.datetime): The current time.

    Returns:
        str: The prepared notification message.
    """

    formatted_last_modified = last_modified.strftime("%Y-%m-%d %H:%M:%S")
    time_diff = now - last_modified
    if time_diff.total_seconds() / 3600 > check_frequency:
        message = f"""
        :warning: {dataset_id}.{table_id} was last modified at {formatted_last_modified}. It hasn't been updated for more than {check_frequency} hours.
        """
    else:
        message = f"""
        :white_check_mark: {dataset_id}.{table_id} was last modified at {formatted_last_modified}. It has been updated within the last {check_frequency} hours.
        """
    return message

def notify_table_updates(now, bq_client, slack_client):
    """Send notifications about table updates.

    Args:
        now (datetime.datetime): The current time.
        bq_client (BigQueryClient): A BigQueryClient instance.
        slack_client (SlackNotifier): A SlackNotifier instance.
        email_client (EmailNotifier): An EmailNotifier instance.
    """

    for table in config["tables"]:
        dataset_id = table["dataset_id"]
        table_id = table["table_id"]
        logging.info(f"start checking last modified time for {dataset_id}.{table_id}")

        check_frequency = table["check_frequency"]
        check_time = datetime.datetime.strptime(table["check_time"], "%H:%M").time()

        if "date_suffix" in table:
            date_suffix = target_date(table["date_suffix"], now)
            table_id = f"{table_id}_{date_suffix.strftime('%Y%m%d')}"

        last_modified = bq_client.check_last_modified_time(dataset_id, table_id)

        if last_modified is None:
            message = f":warning: {dataset_id}.{table_id} does not exist."
            slack_client.send_notification(config["slack"]["channel"], message)
            continue

        message = prepare_notification_message(dataset_id, table_id, last_modified, check_frequency, now)
        slack_client.send_notification(config["slack"]["channel"], message)


def main():
    now = datetime.datetime.now(tz.UTC)
    bq_client = BigQueryClient(config["bigquery"]["project_id"])
    slack_client = SlackNotifier(SLACK_TOKEN)

    notify_table_updates(now, bq_client, slack_client)

if __name__ == "__main__":
    main()