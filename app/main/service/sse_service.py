import hashlib

import pandas as pd
from sqlalchemy import func, select, update

from app.main.config import app_config
from app.main.exceptions import DefaultException
from app.main.model import Database, User
from app.main.service.database_service import get_database_url, get_sensitive_columns
from app.main.service.global_service import (
    TableConnection,
    create_table_connection,
    get_cloud_database_url,
    get_database,
    get_primary_key_name,
)

_batch_selection_size = app_config.BATCH_SELECTION_SIZE


def update_hash_column(
    cloud_table_connection: TableConnection,
    primary_key_name: str,
    primary_key_data: list,
    raw_data: list,
):
    for primary_key_value, row in zip(primary_key_data, range(raw_data.shape[0])):
        record = raw_data.iloc[row].values
        record = list(record)
        new_record = str(record)
        hashed_line = hashlib.sha256(new_record.encode("utf-8")).hexdigest()

        statement = (
            update(cloud_table_connection.table)
            .where(
                cloud_table_connection.get_column(column_name=primary_key_name)
                == primary_key_value
            )
            .values(line_hash=hashed_line)
        )

        cloud_table_connection.session.execute(statement)

    cloud_table_connection.session.flush()


def generate_hash_rows(
    database_id: int, table_name: str, result_query: list[dict], current_user: User
) -> None:
    get_database(database_id=database_id, current_user=current_user)

    # Get primary key name of client database
    primary_key_name = get_primary_key_name(database_id=database_id)

    # Get sensitve columns of table
    client_columns_list = [primary_key_name] + get_sensitive_columns(
        database_id=database_id, table_name=table_name
    )["sensitive_column_names"]

    # Get cloud database url
    cloud_database_url = get_cloud_database_url(database_id=database_id)

    # Create cloud table connection
    cloud_table_connection = create_table_connection(
        database_url=cloud_database_url, table_name=table_name
    )

    # Convert result query to list
    for index in len(result_query):
        result_query[index] = list(result_query[index])

    # Transform query rows to dataframe
    raw_data = pd.DataFrame(result_query, columns=client_columns_list)
    primary_key_data = raw_data[primary_key_name]
    raw_data.pop(primary_key_name)

    update_hash_column(
        cloud_table_connection=cloud_table_connection,
        primary_key_name=primary_key_name,
        primary_key_data=primary_key_data,
        raw_data=raw_data,
    )


def generate_hash_column(
    client_database_id: int,
    client_database_url: str,
    table_name: str,
) -> None:
    primary_key_name = get_primary_key_name(
        database_url=client_database_url, table_name=table_name
    )

    client_columns_list = [primary_key_name] + get_sensitive_columns(
        database_id=client_database_id, table_name=table_name
    )["sensitive_column_names"]

    client_table_connection = create_table_connection(
        database_url=client_database_url,
        table_name=table_name,
        columns_list=client_columns_list,
    )

    cloud_database_url = get_cloud_database_url(database_id=client_database_id)

    cloud_table_connection = create_table_connection(
        database_url=cloud_database_url, table_name=table_name
    )

    # Generate hashs
    results_proxy = client_table_connection.session.execute(
        select(client_table_connection.table)
    )

    results = results_proxy.fetchmany(_batch_selection_size)  # Getting rows database

    while results:
        from_db = []

        for result in results:
            from_db.append(list(result))

        raw_data = pd.DataFrame(from_db, columns=client_columns_list)
        primary_key_data = raw_data[primary_key_name]
        raw_data.pop(primary_key_name)

        results = results_proxy.fetchmany(
            _batch_selection_size
        )  # Getting rows database

        update_hash_column(
            cloud_table_connection=cloud_table_connection,
            primary_key_name=primary_key_name,
            primary_key_data=primary_key_data,
            raw_data=raw_data,
        )
