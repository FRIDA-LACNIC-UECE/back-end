import hashlib

import pandas as pd
from sqlalchemy import func, select, update

from app.main.exceptions import DefaultException
from app.main.service.database_service import get_database_url, get_sensitive_columns
from app.main.service.global_service import (
    create_table_session,
    get_cloud_database_url,
    get_database,
    get_index_column_table_object,
    get_primary_key,
)


def update_hash_column(
    session_db,
    table_name,
    primary_key_data,
    raw_data,
):

    for (primary_key_value, row) in zip(primary_key_data, range(raw_data.shape[0])):
        record = raw_data.iloc[row].values
        record = list(record)
        new_record = str(record)
        hashed_line = hashlib.sha256(new_record.encode("utf-8")).hexdigest()

        stmt = (
            update(table_name)
            .where(table_name.c[0] == primary_key_value)
            .values(line_hash=hashed_line)
        )

        session_db.execute(stmt)

    session_db.commit()
    session_db.close()


def generate_hash_rows(
    user_id: int, database_id: int, table_name: str, result_query: list[dict]
) -> None:

    # Get client database
    database = get_database(database_id=database_id)

    # Check user authorization
    if database.user_id != user_id:
        raise DefaultException("unauthorized_user", code=401)

    # Get primary key name of client database
    primary_key_name = get_primary_key(database_id=database_id)

    # Get sensitve columns of table
    client_columns_list = [primary_key_name] + get_sensitive_columns(
        database_id=database_id, table_name=table_name
    )["sensitive_column_names"]

    # Get cloud database url
    cloud_database_url = get_cloud_database_url(database_id=database_id)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_database, session_cloud_database = create_table_session(
        database_url=cloud_database_url, table_name=table_name
    )

    for index in len(result_query):
        result_query[index] = list(result_query[index])

    # Transform query rows to dataframe
    raw_data = pd.DataFrame(result_query, columns=client_columns_list)
    primary_key_data = raw_data[primary_key_name]
    raw_data.pop(primary_key_name)

    update_hash_column(
        session_db=session_cloud_database,
        table_name=table_cloud_database,
        primary_key_data=primary_key_data,
        raw_data=raw_data,
    )

    return None


def generate_hash_column(user_id: int, database_id: int, table_name: str) -> None:

    # Get client database
    database = get_database(database_id=database_id)

    # Get client database url
    client_database_url = get_database_url(database_id=database_id)

    # Check user authorization
    if database.user_id != user_id:
        raise DefaultException("unauthorized_user", code=401)

    # Get primary key name of client database
    primary_key_name = get_primary_key(database_id=database_id, table_name=table_name)

    # Get sensitve columns of table
    client_columns_list = [primary_key_name] + get_sensitive_columns(
        database_id=database_id, table_name=table_name
    )["sensitive_column_names"]

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_database, session_client_database = create_table_session(
        database_url=client_database_url,
        table_name=table_name,
        columns_list=client_columns_list,
    )

    # Get Cloud Database Url
    cloud_database_url = get_cloud_database_url(database_id=database_id)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_database, session_cloud_database = create_table_session(
        database_url=cloud_database_url, table_name=table_name
    )

    # Commit changes on Cloud Database
    session_cloud_database.commit()
    session_cloud_database.close()

    # Generate hashs
    size = 1000
    statement = select(table_client_database)
    results_proxy = session_client_database.execute(
        statement
    )  # Proxy to get data on batch
    results = results_proxy.fetchmany(size)  # Getting data

    while results:
        from_db = []

        for result in results:
            from_db.append(list(result))

        session_client_database.close()

        raw_data = pd.DataFrame(from_db, columns=client_columns_list)
        primary_key_data = raw_data[primary_key_name]
        raw_data.pop(primary_key_name)

        results = results_proxy.fetchmany(size)  # Getting data

        update_hash_column(
            session_db=session_cloud_database,
            table_name=table_cloud_database,
            primary_key_data=primary_key_data,
            raw_data=raw_data,
        )

    return None


def include_hash_rows(
    user_id: int, database_id: int, table_name: str, hash_rows
) -> None:
    # Get client database
    database = get_database(database_id=database_id)

    # Get Cloud Database Url
    cloud_database_url = get_cloud_database_url(database_id=database_id)

    # Check user authorization
    if database.user_id != user_id:
        raise DefaultException("unauthorized_user", code=401)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_database, session_cloud_database = create_table_session(
        database_url=cloud_database_url,
        table_name=table_name,
    )

    # Get primary key column name
    primary_key_name = get_primary_key(database_id=database_id, table_name=table_name)

    # Get primary key index in table object
    index_primary_key = get_index_column_table_object(
        table_object=table_cloud_database, column_name=primary_key_name
    )

    for row in hash_rows:
        statement = (
            update(table_cloud_database)
            .where(table_cloud_database.c[index_primary_key] == row[primary_key_name])
            .values(line_hash=row["line_hash"])
        )

        session_cloud_database.execute(statement)

    session_cloud_database.commit()

    return (200, "hash_included")
