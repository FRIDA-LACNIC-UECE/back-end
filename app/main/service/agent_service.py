import hashlib

import pandas as pd
from sqlalchemy import func, select, update
from werkzeug.datastructures import ImmutableMultiDict

from app.main.exceptions import DefaultException
from app.main.model import DatabaseKey, User
from app.main.service.database_service import (
    get_database,
    get_database_url,
    get_sensitive_columns,
)
from app.main.service.encryption_service import decrypt_dict, load_keys
from app.main.service.global_service import (
    create_table_session,
    get_cloud_database_url,
    get_index_column_table_object,
    get_primary_key,
)

"""def row_search(
    database_id: int,
    table_name: str,
    search_type: str,
    search_value: any,
    current_user: User,
) -> dict:

    # Get client database
    cliente_database = get_database(database_id=database_id)

    # Check user authorization
    if cliente_database.user != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    # Get cloud database url
    cloud_database_url = get_cloud_database_url(database_id=database_id)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_database, session_cloud_database = create_table_session(
        database_url=cloud_database_url, table_name=table_name
    )

    # Get primary key name of Client Database
    primary_key_name = get_primary_key(database_id=database_id, table_name=table_name)

    # Searching to row on Cloud Database
    if search_type == "primary_key":
        index_column_to_search = get_index_column_table_object(
            table_object=table_cloud_database,
            column_name=primary_key_name,
        )
        query_sensitive_data = (
            session_cloud_database.query(table_cloud_database)
            .filter(table_cloud_database.c[index_column_to_search] == search_value)
            .all()
        )
    elif search_type == "hash":
        index_column_to_search = get_index_column_table_object(
            table_object=table_cloud_database,
            column_name="line_hash",
        )
        query_sensitive_data = (
            session_cloud_database.query(table_cloud_database)
            .filter(table_cloud_database.c[index_column_to_search] == search_value)
            .all()
        )
    else:
        return (None, 400, "search_invalid_data")

    if not query_sensitive_data:
        raise DefaultException("row_not_found", code=404)

    # Transform sensitive data query to dictionary
    dict_sensitive_data = [row._asdict() for row in query_sensitive_data][0]

    # Remove primary key and hash value
    primary_key_value = dict_sensitive_data[f"{primary_key_name}"]
    hash_value = dict_sensitive_data["line_hash"]
    remove_primary_key_response = dict_sensitive_data.pop(primary_key_name, None)
    remove_line_hash_response = dict_sensitive_data.pop("line_hash", None)

    if remove_primary_key_response == None or remove_line_hash_response == None:
        raise DefaultException("internal_server_error", code=500)

    # Load encryption keys
    database_keys = DatabaseKey.query.filter_by(database_id=database_id).first()

    if not database_keys:
        raise DefaultException("database_keys_not_found", code=404)

    _, private_key = load_keys(
        public_key_str=database_keys.public_key,
        private_key_str=database_keys.private_key,
    )

    # Decrypt sensitive data
    dict_decrypted_sensitive_data = decrypt_dict(
        data_dict=dict_sensitive_data, key=private_key
    )

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, table_name=table_name
    )

    # Searching to row on Client Database
    index_column_to_search = get_index_column_table_object(
        table_object=table_client_db,
        column_name=primary_key_name,
    )

    query_non_sensitive_data = (
        session_client_db.query(table_client_db)
        .filter(table_client_db.c[index_column_to_search] == search_value)
        .all()
    )

    if not query_non_sensitive_data:
        raise DefaultException("row_not_found", code=404)

    # Transform non-sensitive data query to dictionary
    dict_result_data = [row._asdict() for row in query_non_sensitive_data][0]

    # Insert decrypted sensitive data in non-sensitive data dictionary
    for key in dict_decrypted_sensitive_data.keys():
        dict_result_data[key] = dict_decrypted_sensitive_data[key]

    # Get date type columns on news rows of Client Database
    data_type_keys = []
    for key in dict_result_data.keys():
        type_data = str(type(dict_result_data[key]).__name__)
        if type_data == "date":
            data_type_keys.append(key)

    # Fix columns date types of row
    for key in data_type_keys:
        dict_result_data[key] = dict_result_data[key].strftime("%Y-%m-%d")

    # Fix columns types of row
    columns_types = get_database_columns_types(
        id_db_user=id_db_user, id_db=id_db, table_name=table_name
    )[0]

    for key in columns_types.keys():
        column_type = columns_types[key].split("(")[0]

        if column_type == "INTEGER":
            dict_result_data[key] = int(dict_result_data[key])
        elif column_type == "VARCHAR":
            dict_result_data[key] = str(dict_result_data[key])
        else:
            pass

    return (dict_result_data, 200, "row_found")"""


def show_rows_hash(
    params: ImmutableMultiDict, database_id: int, current_user: User
) -> tuple:

    table_name = params.get("table_name", type=str)
    page = params.get("page", type=int)
    per_page = params.get("per_page", type=int)

    # Get clint database
    client_database = get_database(database_id=database_id)

    # Get cloud database url
    cloud_database_url = get_cloud_database_url(database_id=database_id)

    # Check authorization user
    if client_database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    # Get primary key name in table object
    primary_key_name = get_primary_key(database_id=database_id, table_name=table_name)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_database, session_cloud_database = create_table_session(
        database_url=cloud_database_url,
        table_name=table_name,
        columns_list=[primary_key_name, "line_hash"],
    )

    # Get primary key index in table object
    index_primary_key = get_index_column_table_object(
        table_object=table_cloud_database, column_name=primary_key_name
    )

    # Run paginate
    query = session_cloud_database.query(table_cloud_database).filter(
        table_cloud_database.c[index_primary_key] >= (page * per_page),
        table_cloud_database.c[index_primary_key] <= ((page + 1) * per_page),
    )

    row_hash_list = {}
    row_hash_list["primary_key"] = []
    row_hash_list["row_hash"] = []

    for row in query:
        row_hash_list["primary_key"].append(row[0])
        row_hash_list["row_hash"].append(row[1])

    # Get limits primary key value
    primary_key_value_min_limit = session_cloud_database.query(
        func.min(table_cloud_database.c[index_primary_key])
    ).scalar()
    primary_key_value_max_limit = session_cloud_database.query(
        func.max(table_cloud_database.c[index_primary_key])
    ).scalar()

    session_cloud_database.commit()
    session_cloud_database.close()

    return {
        "row_hash_list": [row_hash_list],
        "primary_key_value_min_limit": primary_key_value_min_limit,
        "primary_key_value_max_limit": primary_key_value_max_limit,
    }


def include_hash_rows(
    database_id: int, data: dict[str, str], current_user: User
) -> tuple:

    table_name = data.get("table_name")
    hash_rows = data.get("hash_rows")

    # Get client database
    database = get_database(database_id=database_id)

    # Get Cloud Database Url
    cloud_database_url = get_cloud_database_url(database_id=database_id)

    # Check user authorization
    if database.user_id != current_user.id:
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


"""def process_updates(
    id_db_user: int, id_db: int, table_name: str, primary_key_list: list
) -> tuple:

    # Check request body
    if (
        (id_db == None)
        or (table_name == None)
        or (primary_key_list == None)
        or (len(primary_key_list) == 0)
    ):
        return (400, "invalid_updates_data5")

    # Get client database path
    src_client_db_path = get_database_path(id_db)
    if not src_client_db_path:
        return (404, "database_not_found")

    # Check user authorization
    if get_database_user_id(id_db=id_db) != id_db_user:
        return (401, "user_unauthorized")

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, table_name=table_name
    )

    # Get database information by id
    src_cloud_db_path = get_cloud_database_path(id_db=id_db)

    # Get sensitive columns of Client Database
    sensitive_columns = get_sensitive_columns(
        id_db_user=id_db_user, id_db=id_db, table_name=table_name
    )[0]

    # Get original rows ​​that have been updated
    rows_list = []

    for primary_value in primary_key_list:
        found_row = row_search(
            id_db_user=id_db_user,
            id_db=id_db,
            table_name=table_name,
            search_type="primary_key",
            search_value=primary_value,
        )
        found_row = found_row[0]
        print(f"\nfound_row -->> {found_row}\n")

        anonymized_row = found_row.copy()
        anonymized_row, status_code, _ = anonymization_database_rows(
            id_db=id_db,
            table_name=table_name,
            rows_to_anonymization=[anonymized_row],
            insert_database=False,
        )
        anonymized_row = anonymized_row[0]

        stmt = select(table_client_db).where(table_client_db.c[0] == primary_value)
        client_row = session_client_db.execute(stmt)
        client_row = [row._asdict() for row in client_row][0]
        print(f"\nclient_row -->> {client_row}\n")
        print(f"\nanonymized_row -->> {anonymized_row}\n")

        for sensitive_column in sensitive_columns:

            if type(client_row[sensitive_column]).__name__ == "date":
                print("entrei1")
                if anonymized_row[sensitive_column] != client_row[
                    sensitive_column
                ].strftime("%Y-%m-%d"):
                    found_row[sensitive_column] = client_row[sensitive_column]
                    print(f"###TROCOU### -> {sensitive_column}")
            else:
                print("entrei2")
                if anonymized_row[sensitive_column] != client_row[sensitive_column]:
                    found_row[sensitive_column] = client_row[sensitive_column]
                    print(f"###TROCOU### -> {sensitive_column}")

        rows_list.append(found_row)

    encrypt_database_rows(
        id_db_user=id_db_user,
        id_db=id_db,
        rows_to_encrypt=rows_list.copy(),
        table_name=table_name,
        update_database=True,
    )

    anonymized_rows, _, _ = anonymization_database_rows(
        id_db=id_db,
        table_name=table_name,
        rows_to_anonymization=rows_list.copy(),
        insert_database=True,
    )
    print(f"\anonymized_rows -->> {anonymized_rows}\n")

    generate_hash_rows(
        id_db_user=id_db_user,
        id_db=id_db,
        table_name=table_name,
        result_query=anonymized_rows,
    )

    if status_code == 200:
        return (200, "updates_processed")"""


def process_deletions(
    database_id: int, data: dict[str, str], current_user: User
) -> None:

    table_name = data.get("table_name")
    primary_key_list = data.get("primary_key_list")

    # Get client database
    client_database = get_database(database_id=database_id)

    # Check user authorization
    if client_database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    # Get database information by id
    cloud_database_url = get_cloud_database_url(database_id=database_id)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_database, session_cloud_database = create_table_session(
        database_url=cloud_database_url, table_name=table_name
    )

    for primary_key in primary_key_list:
        session_cloud_database.query(table_cloud_database).filter(
            table_cloud_database.c[0] == primary_key
        ).delete()

    session_cloud_database.commit()
    session_cloud_database.close()
