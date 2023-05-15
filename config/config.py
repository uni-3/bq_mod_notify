import logging
import yaml

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
