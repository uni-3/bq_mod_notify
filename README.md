# BigQuery Table Monitor

BigQuery Table Monitor is a Python package that monitors the last update time of BigQuery tables and sends notifications via Slack.

## Features

- Monitors the last update time of BigQuery tables.
- Configurable check frequency and time for each dataset and table using a YAML configuration file.
- Uses INFORMATION_SCHEMA to determine if the last update time is older than a specified threshold.
- Sends notifications for each table's success or failure to a Slack channel.

## Configuration
Configure the monitoring settings using the config.yml file. Example configuration: see [example config](config/config.yml)