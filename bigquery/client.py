import logging
import datetime

from date_utils.date import target_date
from google.cloud import bigquery


class BigQueryClient:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    def check_last_modified_time(self, dataset_id, table_id):
        query = f"""
          SELECT
            max(last_modified_time) as last_modified
          FROM
            `{self.project_id}.{dataset_id}.INFORMATION_SCHEMA.PARTITIONS`
          WHERE table_name = '{table_id}'
          GROUP BY table_name
        """
        query_job = self.client.query(query)
        result = query_job.result()
        result_list = list(result)
        logging.info("query", query)
        if len(result_list) == 0:
            logging.warning(f"No result found for {dataset_id}.{table_id}")

            return None
        row = result_list[0]
        return row["last_modified"]

    def prepare_notification_message(self, dataset_id, table_id, last_modified, check_frequency, now):
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


    def notify_table_updates(self, dataset_id, table_id, check_frequency, check_time, now, date_suffix=None):
        """Send notifications about table updates.

        Args:
            dataset_id:  
            table_id:  
            check_frequency:  
            check_time:  
            now (datetime.datetime): The current time.
            date_suffix:
        """

        logging.info(f"start checking last modified time for {dataset_id}.{table_id}")

        check_time = datetime.datetime.strptime(check_time, "%H:%M").time()

        if date_suffix is not None:
            date_suffix = target_date(date_suffix, now)
            table_id = f"{table_id}_{date_suffix.strftime('%Y%m%d')}"

        last_modified = self.check_last_modified_time(dataset_id, table_id)

        if last_modified is None:
            message = f":warning: {dataset_id}.{table_id} does not exist."
            return message

        message = self.prepare_notification_message(dataset_id, table_id, last_modified, check_frequency, now)

        return message