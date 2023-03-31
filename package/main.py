import sys
import logging
from pandas import read_csv
from concurrent.futures import ThreadPoolExecutor

import os
import shutil
from pathlib import Path
from faker import Faker

from vertica_python.vertica.cursor import Cursor

# package
sys.path.append(str(Path(__file__).resolve().parent.parent))

from package.utils import Connector, get_dev_logger
from package.errors import S3ServiceError, FileSystemError, VerticaError

# gets airflow default logger and use it
# logger = logging.getLogger("airflow.task")
logger = get_dev_logger(logger_name=str(Path(Path(__file__).name)))


class DWHCreator:
    def __init__(self) -> None:
        self.dwh_conn = Connector(type="dwh")
        self.path_to_sql = Path(Path.cwd(), "sql")

    def create_stg_layer(self) -> None:
        logger.info("Initializing STG layer.")

        SQL = "stg-ddl"

        try:
            logger.info(f"Reading `{SQL}.sql`.")
            query = Path(self.path_to_sql, f"{SQL}.sql").read_text(encoding="UTF-8")
            logger.info(f"`{SQL}.sql` loaded.")
        except Exception:
            logger.exception(
                f"Unable to read `{SQL}.sql`! Initializing process failed."
            )
            raise FileSystemError

        try:
            logger.info(f"Executing DDL query for STG layer.")

            with self.dwh_conn.connect() as conn:
                cur = conn.cursor()
                cur.execute(query)

            logger.info(f"STG layer created successfully.")
        except Exception:
            logger.exception(
                f"Unable to execute DDL query! Initializing process failed."
            )
            raise VerticaError

    def create_dds_layer(self) -> None:
        pass


class DataGetter:
    def __init__(self) -> None:
        self.s3_conn = Connector(type="ice-lake").connect()
        self.path_to_data = Path(Path.cwd(), "data")
        self.faker = Faker(locale="pt_BR")
        # self.files_list = [
        #     "olist_customers_dataset",
        #     "olist_geolocation_dataset",
        #     "olist_order_reviews_dataset",
        #     "olist_orders_dataset",
        #     "olist_products_dataset",
        #     "olist_sellers_dataset",
        #     "product_category_name_translation",
        #     "olist_order_payments_dataset",
        #     "olist_order_items_dataset",
        # ]
        self._check_data_folder()

    def _check_data_folder(self):
        if not Path.exists(self.path_to_data):
            Path.mkdir(self.path_to_data)

    def _download_file_from_s3(self, folder: str, file: str) -> None:

        KEY = f"e-commerce-data/{folder}/{file}.csv"
        S3_BUCKET = "data-ice-lake-04"

        try:
            self.s3_conn.download_file(
                Filename=Path(self.path_to_data, f"{file}.csv"),
                Bucket=S3_BUCKET,
                Key=KEY,
            )
            logger.info(f"Successfully downloaded `{file}` file.")
        except Exception:
            logger.exception(f"Unable to download `{file}` from s3!")
            raise S3ServiceError

    def _upload_file_to_s3(self, file: str) -> None:

        KEY = f"e-commerce-data/prepared-data/{file}.csv"
        S3_BUCKET = "data-ice-lake-04"

        logger.info(f"Uploading `{file}` to s3.")

        try:
            self.s3_conn.upload_file(
                Filename=Path(self.path_to_data, f"{file}.csv"),
                Bucket=S3_BUCKET,
                Key=KEY,
            )
            logger.info(f"Successfully uploaded `{file}` file to s3.")
        except Exception:
            logger.exception(f"Unable to upload `{file}` to s3!")
            raise S3ServiceError

    def _delete_file(self, file: str) -> None:
        try:
            logger.info(f"Removing `{file}`.")
            os.remove(path=Path(self.path_to_data, f"{file}.csv"))
        except Exception:
            logger.exception(f"Unable to remove `{file}`!")
            raise FileSystemError

    def prepare_customers_data(self) -> None:
        FILE_NAME = "olist_customers_dataset"
        FOLDER = "raw-data"

        logger.info(f"Preparing `{FILE_NAME}` data.")

        self._download_file_from_s3(folder=FOLDER, file=FILE_NAME)

        df = read_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"))

        if df.duplicated(subset=["customer_id"], keep=False).sum() > 0:
            df = df.drop_duplicates(subset=["customer_id"], keep=False)
            logger.info(f"Found duplicated values. Dropping.")

        df.customer_city = df.customer_city.str.title()

        try:
            logger.info("Generating additional data.")
            df["customer_name"] = [self.faker.name() for _ in range(df.shape[0])]
            df["customer_email"] = [self.faker.email() for _ in range(df.shape[0])]
            df["phone_number"] = [self.faker.phone_number() for _ in range(df.shape[0])]
            df["date_of_birth"] = [
                str(self.faker.date_of_birth()) for _ in range(df.shape[0])
            ]

        except Exception:
            logger.exception("Unable to generate data and add it to DataFrame!")
            raise Exception

        try:
            logger.info(f"Writing file to `{self.path_to_data}` folder.")
            df.to_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"), index=False)
        except Exception:
            logger.exception("Unable to write file!")
            raise FileSystemError

        self._upload_file_to_s3(file=FILE_NAME)
        self._delete_file(file=FILE_NAME)

    def prepare_geolocations_data(self) -> None:
        FILE_NAME = "olist_geolocation_dataset"
        FOLDER = "raw-data"

        logger.info(f"Preparing `{FILE_NAME}` data.")

        self._download_file_from_s3(folder=FOLDER, file=FILE_NAME)

        df = read_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"))

        if (
            df.duplicated(
                subset=[
                    "geolocation_zip_code_prefix",
                    "geolocation_lat",
                    "geolocation_lng",
                ],
                keep=False,
            ).sum()
            > 0
        ):
            df = df.drop_duplicates(
                subset=[
                    "geolocation_zip_code_prefix",
                    "geolocation_lat",
                    "geolocation_lng",
                ],
                keep=False,
            )
            logger.info(f"Found duplicated values. Dropping.")

        df.geolocation_city = df.geolocation_city.str.title()

        try:
            logger.info(f"Writing file to `{self.path_to_data}` folder.")
            df.to_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"), index=False)
        except Exception:
            logger.exception("Unable to write file!")
            raise FileSystemError

        self._upload_file_to_s3(file=FILE_NAME)
        self._delete_file(file=FILE_NAME)

    def prepare_orders_data(self) -> None:
        FILE_NAME = "olist_orders_dataset"
        FOLDER = "raw-data"

        logger.info(f"Preparing `{FILE_NAME}` data.")

        self._download_file_from_s3(folder=FOLDER, file=FILE_NAME)

        df = read_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"))

        if df.duplicated(subset=["order_id"], keep=False).sum() > 0:
            df = df.drop_duplicates(subset=["order_id"], keep=False)
            logger.info(f"Found duplicated values. Dropping.")

        df.order_status = df.order_status.str.capitalize()

        try:
            logger.info(f"Writing file to `{self.path_to_data}` folder.")
            df.to_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"), index=False)
        except Exception:
            logger.exception("Unable to write file!")
            raise FileSystemError

        self._upload_file_to_s3(file=FILE_NAME)
        self._delete_file(file=FILE_NAME)

    def prepare_products_data(self) -> None:
        FILE_NAME = "olist_products_dataset"
        FOLDER = "raw-data"

        logger.info(f"Preparing `{FILE_NAME}` data.")

        self._download_file_from_s3(folder=FOLDER, file=FILE_NAME)

        df = read_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"))

        if df.duplicated(subset=["product_id"], keep=False).sum() > 0:
            df = df.drop_duplicates(subset=["product_id"], keep=False)
            logger.info(f"Found duplicated values. Dropping.")

        df = df.drop(
            columns=[
                "product_name_lenght",
                "product_description_lenght",
                "product_photos_qty",
            ]
        )
        df.product_category_name = df.product_category_name.str.replace(
            "_", ""
        ).str.capitalize()

        try:
            logger.info("Generating additional data.")
            df["product_name"] = [
                (
                    self.faker.word()
                    + " "
                    + self.faker.word()
                    + " "
                    + self.faker.word()
                ).title()
                for _ in range(df.shape[0])
            ]
        except Exception:
            logger.exception("Unable to generate data and add it to DataFrame!")
            raise Exception

        try:
            logger.info(f"Writing file to `{self.path_to_data}` folder.")
            df.to_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"), index=False)
        except Exception:
            logger.exception("Unable to write file!")
            raise FileSystemError

        self._upload_file_to_s3(file=FILE_NAME)
        self._delete_file(file=FILE_NAME)


class STGDataLoader:
    def __init__(self) -> None:
        self.dwh_conn = Connector(type="dwh").connect()
        self.path_to_data = Path(Path.cwd(), "data")
        self.path_to_sql = Path(Path.cwd(), "sql")

    def _create_temp_table(self, cur: Cursor, sql: str) -> None:

        try:
            logger.info("Creating temp table.")
            cur.execute(sql)

            logger.info("Temp table created.")
        except Exception:
            logger.exception("Unable to create temp table!")
            raise VerticaError

    def _bulk_insert_into_temp_table(
        self, cur: Cursor, sql: str, check_clause: str = None
    ) -> None:

        try:
            logger.info("Inserting data into temp table.")

            cur.execute(sql)

            if check_clause:
                rows_num = cur.execute(check_clause).fetchone()
                logger.info(
                    f"Successfully inserted data into temp table with {rows_num} rows."
                )
            else:
                logger.info(f"Successfully inserted data into temp table.")

        except Exception:
            logger.exception("Unable to insert data into temp table!")
            raise VerticaError

    def _bulk_insert_into_stg_table(
        self, cur: Cursor, sql: str, check_clause: str = None
    ) -> None:

        try:
            logger.info("Bulk insert into stage table.")

            cur.execute(sql)

            if check_clause:
                rows_num = cur.execute(check_clause).fetchone()
                logger.info(
                    f"Successfully inserted data into stage table with {rows_num} rows."
                )
            else:
                logger.info(f"Successfully inserted data into stage table.")

        except Exception:
            logger.exception("Unable to do bulk insert into stage table!")
            raise VerticaError

    def _update_stg_table(self, cur: Cursor, sql: str) -> None:

        try:
            logger.info("Updating stage table.")
            cur.execute(sql)
            logger.info(f"Stage table was successfully updated.")
        except Exception:
            logger.exception("Unable to update stage table!")
            raise VerticaError

    def _drop_temp_table(self, cur: Cursor, sql: str):

        try:
            cur.execute(sql)
            logger.info("Temp table dropped.")
        except Exception:
            logger.exception("Unable to drop temp table!")
            raise VerticaError

    def load_products_data_to_dwh(self) -> None:

        FILE_NAME = "olist_products_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._create_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.products_tmp;
                    CREATE GLOBAL TEMP TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.products_tmp
                    (
                        product_id         varchar(100),
                        category_name      varchar(100),
                        weight_g           float,
                        length_cm          float,
                        height_cm          float,
                        width_cm           float
                    )
                        ON COMMIT PRESERVE ROWS
                        DIRECT
                        UNSEGMENTED ALL NODES
                        INCLUDE SCHEMA PRIVILEGES;
                """,
            )
            self._bulk_insert_into_temp_table(
                cur=cur,
                sql=f"""
                    COPY LEONIDGRISHENKOVYANDEXRU__STAGING.products_tmp(product_id, category_name, weight_g, length_cm, height_cm, width_cm)
                    FROM LOCAL '{PATH_TO_FILE}'
                    DELIMITER ','
                    ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                    FROM LEONIDGRISHENKOVYANDEXRU__STAGING.products_tmp;
                """,
            )
            self._update_stg_table(
                cur=cur,
                sql="""
                    MERGE INTO LEONIDGRISHENKOVYANDEXRU__STAGING.products AS tgt
                    USING LEONIDGRISHENKOVYANDEXRU__STAGING.products_tmp AS src
                    ON tgt.product_id = src.product_id
                    WHEN MATCHED THEN
                        UPDATE
                        SET category_name      = src.category_name,
                            weight_g           = src.weight_g,
                            length_cm          = src.length_cm,
                            height_cm          = src.height_cm,
                            width_cm           = src.width_cm
                    WHEN NOT MATCHED THEN
                        INSERT (product_id, category_name, weight_g, length_cm, height_cm, width_cm)
                        VALUES (src.product_id, src.category_name, src.weight_g, src.length_cm, src.height_cm, src.width_cm);
                """,
            )
            self._drop_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.products_tmp;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_customers_data_to_dwh(self) -> None:

        FILE_NAME = "olist_customers_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._create_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.customers_tmp;
                    CREATE GLOBAL TEMP TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.customers_tmp
                    (
                        customer_id        varchar(100),
                        customer_unique_id varchar(100),
                        zip_code           int,
                        city               varchar(100),
                        state              varchar(50)
                    )
                        ON COMMIT PRESERVE ROWS
                        DIRECT
                        UNSEGMENTED ALL NODES
                        INCLUDE SCHEMA PRIVILEGES;
                """,
            )
            self._bulk_insert_into_temp_table(
                cur=cur,
                sql=f"""
                    COPY LEONIDGRISHENKOVYANDEXRU__STAGING.customers_tmp (customer_id, customer_unique_id, zip_code, city, state)
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                    FROM LEONIDGRISHENKOVYANDEXRU__STAGING.customers_tmp;
                """,
            )
            self._update_stg_table(
                cur=cur,
                sql="""
                    MERGE INTO LEONIDGRISHENKOVYANDEXRU__STAGING.customers AS tgt
                    USING LEONIDGRISHENKOVYANDEXRU__STAGING.customers_tmp AS src
                    ON tgt.customer_id = src.customer_id
                    WHEN MATCHED THEN
                        UPDATE
                        SET customer_unique_id = src.customer_unique_id,
                            zip_code           = src.zip_code,
                            city               = src.city,
                            state              = src.state
                    WHEN NOT MATCHED THEN
                        INSERT (customer_id, customer_unique_id, zip_code, city, state)
                        VALUES (src.customer_id, src.customer_unique_id, src.zip_code, src.city, src.state);
                """,
            )
            self._drop_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.customers_tmp;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_geolocation_data_to_dwh(self) -> None:

        FILE_NAME = "olist_geolocation_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._create_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation_tmp;
                    CREATE GLOBAL TEMP TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation_tmp
                    (
                        zip_code  int NOT NULL UNIQUE ENABLED,
                        latitude  float,
                        longitude float,
                        city      varchar(100),
                        state     varchar(50)
                    )
                        ON COMMIT PRESERVE ROWS
                        DIRECT
                        UNSEGMENTED ALL NODES
                        INCLUDE SCHEMA PRIVILEGES;
                """,
            )
            self._bulk_insert_into_temp_table(
                cur=cur,
                sql=f"""
                    COPY LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation_tmp (zip_code, latitude, longitude, city, state)
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                        FROM LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation_tmp;
                """,
            )
            self._update_stg_table(
                cur=cur,
                sql="""
                    MERGE INTO LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation AS tgt
                    USING LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation_tmp AS src
                    ON tgt.zip_code = src.zip_code
                    WHEN MATCHED THEN
                        UPDATE
                        SET latitude  = src.latitude,
                            longitude = src.longitude,
                            city      = src.city,
                            state     = src.state
                    WHEN NOT MATCHED THEN
                        INSERT (zip_code, latitude, longitude, city, state)
                        VALUES (src.zip_code, src.latitude, src.longitude, src.city, src.state);
                """,
            )
            self._drop_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation_tmp;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_sellers_data_to_dwh(self) -> None:

        FILE_NAME = "olist_sellers_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._create_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.sellers_tmp;
                    CREATE GLOBAL TEMP TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.sellers_tmp
                    (
                        seller_id varchar(100) NOT NULL UNIQUE ENABLED,
                        zip_code  int,
                        city      varchar(100),
                        state     varchar(50)
                    )
                        ON COMMIT PRESERVE ROWS
                        DIRECT
                        UNSEGMENTED ALL NODES
                        INCLUDE SCHEMA PRIVILEGES;
                """,
            )
            self._bulk_insert_into_temp_table(
                cur=cur,
                sql=f"""
                    COPY LEONIDGRISHENKOVYANDEXRU__STAGING.sellers_tmp (seller_id, zip_code, city, state)
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                    FROM LEONIDGRISHENKOVYANDEXRU__STAGING.sellers_tmp;
                """,
            )
            self._update_stg_table(
                cur=cur,
                sql="""
                    MERGE INTO LEONIDGRISHENKOVYANDEXRU__STAGING.sellers AS tgt
                    USING LEONIDGRISHENKOVYANDEXRU__STAGING.sellers_tmp AS src
                    ON tgt.seller_id = src.seller_id
                    WHEN MATCHED THEN
                        UPDATE
                        SET zip_code = src.zip_code,
                            city = src.city,
                            state = src.state
                    WHEN NOT MATCHED THEN
                        INSERT (seller_id, zip_code, city, state)
                        VALUES (src.seller_id, src.zip_code, src.city, src.state);
                """,
            )
            self._drop_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.sellers_tmp;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_reviews_data_to_dwh(self) -> None:

        FILE_NAME = "olist_order_reviews_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._create_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.reviews_tmp;
                    CREATE GLOBAL TEMP TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.reviews_tmp
                    (
                        review_id        varchar(100) NOT NULL UNIQUE ENABLED,
                        order_id         varchar(100),
                        review_score     int,
                        comment_title    varchar(100),
                        comment_message  varchar(300),
                        creation_date    timestamp,
                        answer_timestamp timestamp
                    )
                        ON COMMIT PRESERVE ROWS
                        DIRECT
                        UNSEGMENTED ALL NODES
                        INCLUDE SCHEMA PRIVILEGES;
                """,
            )
            self._bulk_insert_into_temp_table(
                cur=cur,
                sql=f"""
                    COPY LEONIDGRISHENKOVYANDEXRU__STAGING.reviews_tmp
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                    FROM LEONIDGRISHENKOVYANDEXRU__STAGING.reviews_tmp;
                """,
            )
            self._update_stg_table(
                cur=cur,
                sql="""
                    MERGE INTO LEONIDGRISHENKOVYANDEXRU__STAGING.reviews AS tgt
                    USING LEONIDGRISHENKOVYANDEXRU__STAGING.reviews_tmp AS src
                    ON tgt.review_id = src.review_id
                    WHEN MATCHED THEN
                        UPDATE
                        SET order_id         = src.order_id,
                            review_score     = src.review_score,
                            comment_title    = src.comment_title,
                            comment_message  = src.comment_message,
                            creation_date    = src.creation_date,
                            answer_timestamp = src.answer_timestamp
                    WHEN NOT MATCHED THEN
                        INSERT (review_id, order_id, review_score, comment_title, comment_message, creation_date, answer_timestamp)
                        VALUES (src.review_id, src.order_id, src.review_score, src.comment_title, src.comment_message, src.creation_date, src.answer_timestamp);
                """,
            )
            self._drop_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.reviews_tmp;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_payments_data_to_dwh(self) -> None:

        FILE_NAME = "olist_order_payments_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._bulk_insert_into_stg_table(
                cur=cur,
                sql=f"""
                    COPY LEONIDGRISHENKOVYANDEXRU__STAGING.payments
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                        FROM LEONIDGRISHENKOVYANDEXRU__STAGING.payments;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_order_items_data_to_dwh(self) -> None:

        FILE_NAME = "olist_order_items_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._bulk_insert_into_stg_table(
                cur=cur,
                sql=f"""
                    COPY LEONIDGRISHENKOVYANDEXRU__STAGING.order_items
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                        FROM LEONIDGRISHENKOVYANDEXRU__STAGING.order_items;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_orders_data_to_dwh(self) -> None:

        FILE_NAME = "olist_orders_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._create_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.orders_tmp;
                    CREATE GLOBAL TEMP TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.orders_tmp
                    (
                        order_id                      varchar(100) NOT NULL UNIQUE ENABLED,
                        customer_id                   varchar(100),
                        order_status                  varchar(50),
                        order_purchase_timestamp      timestamp,
                        order_approved_at             timestamp,
                        order_delivered_carrier_date  timestamp,
                        order_delivered_customer_date timestamp,
                        order_estimated_delivery_date timestamp
                    )
                        ON COMMIT PRESERVE ROWS
                        DIRECT
                        UNSEGMENTED ALL NODES
                        INCLUDE SCHEMA PRIVILEGES;
                """,
            )
            self._bulk_insert_into_temp_table(
                cur=cur,
                sql=f"""
                    COPY LEONIDGRISHENKOVYANDEXRU__STAGING.orders_tmp
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                        FROM LEONIDGRISHENKOVYANDEXRU__STAGING.orders_tmp;
                """,
            )
            self._update_stg_table(
                cur=cur,
                sql="""
                    MERGE INTO LEONIDGRISHENKOVYANDEXRU__STAGING.orders AS tgt
                    USING LEONIDGRISHENKOVYANDEXRU__STAGING.orders_tmp AS src
                    ON tgt.order_id = src.order_id
                    WHEN MATCHED THEN
                        UPDATE
                        SET
                            customer_id                   = src.customer_id,
                            order_status                  = src.order_status,
                            order_purchase_timestamp      = src.order_purchase_timestamp,
                            order_approved_at             = src.order_approved_at,
                            order_delivered_carrier_date  = src.order_delivered_carrier_date,
                            order_delivered_customer_date = src.order_delivered_customer_date,
                            order_estimated_delivery_date = src.order_estimated_delivery_date
                    WHEN NOT MATCHED THEN
                        INSERT (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date,
                                order_delivered_customer_date, order_estimated_delivery_date)
                        VALUES (src.order_id, src.customer_id, src.order_status, src.order_purchase_timestamp, src.order_approved_at,
                                src.order_delivered_carrier_date, src.order_delivered_customer_date, src.order_estimated_delivery_date);
                """,
            )
            self._drop_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.orders_tmp;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")


def do_testing() -> None:

    logger.info("Lets do some testing. 🤓")

    # creator = DWHCreator()
    # creator.create_stg_layer()

    getter = DataGetter()
    # getter.prepare_customers_data()
    # getter.prepare_geolocations_data()
    # getter.prepare_orders_data()
    getter.prepare_products_data()

    # data_loader = STGDataLoader()
    # data_loader.load_products_data_to_dwh()
    # data_loader.load_customers_data_to_dwh()
    # data_loader.load_geolocation_data_to_dwh()
    # data_loader.load_sellers_data_to_dwh()
    # data_loader.load_reviews_data_to_dwh()
    # data_loader.load_payments_data_to_dwh()
    # data_loader.load_order_items_data_to_dwh()
    # data_loader.load_orders_data_to_dwh()


if __name__ == "__main__":
    do_testing()
