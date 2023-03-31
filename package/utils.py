import sys
import boto3

# logging
import logging

# typing
from enum import StrEnum
from vertica_python import Connection
from boto3.session import Session

import vertica_python

# fs
from pathlib import Path

import coloredlogs
from dotenv import load_dotenv, find_dotenv
from os import getenv


# package
sys.path.append(str(Path(__file__).resolve().parent.parent))

from package.errors import DotEnvError, S3ServiceError, VerticaError


def get_dev_logger(logger_name: str) -> logging.Logger:
    """This logger is for development and testing purposes, don't use it in airflow production environment."""

    logger = logging.getLogger(name=logger_name)

    coloredlogs.install(logger=logger, level="INFO")
    logger.setLevel(level=logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger_handler = logging.StreamHandler(stream=sys.stdout)

    colored_formatter = coloredlogs.ColoredFormatter(
        fmt="[%(asctime)s UTC] {%(name)s:%(lineno)d} %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level_styles=dict(
            info=dict(color="green"),
            error=dict(color="red", bold=False, bright=True),
        ),
        field_styles=dict(
            asctime=dict(color="magenta"),
            name=dict(color="cyan"),
            levelname=dict(color="yellow", bold=True, bright=True),
            lineno=dict(color="white"),
        ),
    )

    logger_handler.setFormatter(fmt=colored_formatter)
    logger.addHandler(logger_handler)
    logger.propagate = False

    return logger


logger = get_dev_logger(logger_name=str(Path(Path(__file__).name)))


class Connector:
    def __init__(self, type: StrEnum("dwh", "ice-lake")) -> None:
        self.type = type

    def _get_credentials(self) -> dict:
        logger.info("Getting environment variables from .env file.")
        try:
            find_dotenv(raise_error_if_not_found=True)
            logger.info(".env was found.")
        except Exception:
            logger.exception("Can't find .env file!")
            raise DotEnvError

        try:
            load_dotenv(verbose=True, override=True)
            logger.info(".env loaded.")
        except Exception:
            logger.exception("Unable to read .env! Please check if its accessible.")
            raise DotEnvError

        if self.type == "dwh":
            try:
                holder = dict(
                    host=getenv("VERTICA_DWH_HOST"),
                    port=getenv("VERTICA_DWH_PORT"),
                    user=getenv("VERTICA_DWH_USER"),
                    password=getenv("VERTICA_DWH_PASSWORD"),
                    database=getenv("VERTICA_DWH_DB"),
                    autocommit=False,
                    log_level=logging.INFO,
                    log_path="",
                )
                logger.info(f"All variables loaded for `{self.type}` connection.")
            except Exception:
                logger.exception(
                    f"Unable to get one of or all variables for `{self.type}` connection!"
                )
                raise DotEnvError

        if self.type == "ice-lake":
            try:
                holder = dict(
                    key=getenv("S3_ACCESS_KEY"),
                    secret=getenv("S3_SECRET_KEY"),
                    endpoint="https://storage.yandexcloud.net",
                )
                logger.info(f"All variables loaded for `{self.type}` connection.")
            except Exception:
                logger.exception(
                    f"Unable to get one of or all variables for `{self.type}` connection!"
                )
                raise DotEnvError

        return holder

    def connect(self) -> Connection | Session:

        holder = self._get_credentials()

        if self.type == "dwh":
            logger.info(f"Connecting to vertica `{holder['database']}` database.")

            try:
                conn = vertica_python.connect(**holder)
                logger.info(
                    f"Succesfully connected to vertica `{holder['database']}` database."
                )
            except Exception:
                logger.exception(
                    f"Connection to vertica `{holder['database']}` database failed!"
                )
                raise VerticaError

        if self.type == "ice-lake":
            logger.info(f"Connecting to s3 service.")

            try:
                session = boto3.session.Session()

                conn = session.client(
                    service_name="s3",
                    region_name="ru-central1",
                    endpoint_url=holder["endpoint"],
                    aws_access_key_id=holder["key"],
                    aws_secret_access_key=holder["secret"],
                )
                logger.info(f"Succesfully connected to s3 service.")
            except Exception:
                logger.exception("Connection to s3 service failed!")
                raise S3ServiceError

        return conn


def do_testing() -> None:
    conn = Connector(type="dwh").connect()
    v = "v_catalog"

    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            table_schema,
            table_name,
            column_name
        FROM {v}.columns;
        """
    )
    res = cur.fetchall()
    conn.close()
    print(res)

    conn = Connector(type="ice-lake").connect()


if __name__ == "__main__":
    do_testing()
