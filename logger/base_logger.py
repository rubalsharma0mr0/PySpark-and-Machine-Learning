import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv


class CustomFormatter(logging.Formatter):
    """
    Custom log formatter that adds color to log messages based on their log level.
    """
    grey = '\x1b[38;21m'
    blue = '\x1b[38;5;39m'
    yellow = '\x1b[38;5;226m'
    red = '\x1b[38;5;196m'
    bold_red = '\x1b[31;1m'
    reset = '\x1b[0m'

    def __init__(self, fmt):
        """
        Initialize the CustomFormatter object.

        Args:
            fmt (str): The log format string.
        """
        super().__init__()
        self.fmt = fmt
        self.FORMATS = {
            logging.DEBUG: self.grey + self.fmt + self.reset,
            logging.INFO: self.blue + self.fmt + self.reset,
            logging.WARNING: self.yellow + self.fmt + self.reset,
            logging.ERROR: self.red + self.fmt + self.reset,
            logging.CRITICAL: self.bold_red + self.fmt + self.reset
        }

    def format(self, record):
        """
        Format the log record.

        Args:
            record (logging.LogRecord): The log record to be formatted.

        Returns:
            str: The formatted log record.
        """
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class Logger:
    ger = None

    @classmethod
    def log(cls):
        """
        Retrieves or creates a logger instance and configures it based on the environment variables.

        Returns:
            logging.Logger: The logger instance.
        """
        if cls.ger is None:
            # Load environment variables from config.env file
            path_to_config = f"../config.env"
            env_path = Path(path_to_config)
            load_dotenv(env_path)

            # Get log level from environment variable
            log_level = str(os.getenv("LOG_LEVEL")).lower()

            # Get logger name from environment variable, default to 'PySparkEngine'
            logger_name = os.getenv("APPLICATION_NAME")
            if not logger_name:
                logger_name = 'PySparkEngine'

            log_format = '%(asctime)s - [%(filename)s:%(lineno)d:%(funcName)s] - %(levelname)s - %(message)s'

            # Create root logger and set its log level
            root_logger = logging.getLogger(logger_name)
            log_level_mapping = {
                "warning": logging.WARNING,
                "error": logging.ERROR,
                "critical": logging.CRITICAL,
                "debug": logging.DEBUG,
                "fatal": logging.FATAL
            }
            root_logger.setLevel(log_level_mapping.get(log_level, logging.INFO))

            # Create custom log formatter
            log_formatter = CustomFormatter(log_format)

            # Create logs directory if it doesn't exist
            if not os.path.exists("../logs"):
                os.makedirs("../logs")

            # Create file handler for logging to a file
            file_handler = logging.FileHandler(
                f"../logs/BaseLogger_{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log")
            file_handler.setFormatter(log_formatter)
            root_logger.addHandler(file_handler)

            # Create console handler for logging to console
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(log_formatter)
            root_logger.addHandler(console_handler)

            # Store the logger instance in the class variable
            cls.ger = root_logger

        return cls.ger
