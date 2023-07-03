import base64
from sqlalchemy import func
import pandas as pd
import rsa
import time
from app.main import db
from sqlalchemy import create_engine, insert, select, update
from sqlalchemy_utils import create_database, database_exists, drop_database

from app.main.config import app_config
from app.main.exceptions import DefaultException, ValidationException
from app.main.model import DatabaseKey, User, Table
from app.main.service.database_key_service import load_keys
from app.main.service.database_service import (
    get_database,
    get_database_columns_types,
    get_sensitive_columns,
)
from app.main.service.global_service import (
    create_table_connection,
    get_cloud_database_url,
    get_primary_key_name,
)
from app.main.service.table_service import get_table

_batch_selection_size = app_config.BATCH_SELECTION_SIZE


def _calculate_progress(table: Table, number_row_selected: int, number_row_total: int):
    table.encryption_progress = (number_row_selected // number_row_total) * 100
    db.session.commit()


def encrypt(message, key):
    # Encrypt message
    encrypted_message = rsa.encrypt(message.encode(), key)

    # Convert from byte to base64
    base64_encrypted_message = base64.b64encode(encrypted_message)

    return str(base64_encrypted_message)[
        2:-1
    ]  # Remove caracter "b'" [0-1] and "'"[-1] of byte format.


def decrypt(encrypted_message, key):
    # Convert from string (base64) to bytes
    ciphertext = base64.b64decode(encrypted_message.encode())

    return rsa.decrypt(ciphertext, key).decode()


def encrypt_list(data_list, key):
    encrypted_list = []

    for data in data_list:
        data = str(data)
        encrypted_list.append(encrypt(data, key))

    return encrypted_list


def decrypt_list(data_list, key):
    decrypted_list = []

    for data in data_list:
        data = str(data)
        decrypted_list.append(decrypt(data, key))

    return decrypted_list


def encrypt_dict(data_dict, key):
    for keys in data_dict.keys():
        data_dict[keys] = encrypt(str(data_dict[keys]), key)

    return data_dict


def decrypt_dict(data_dict, key):
    for keys in data_dict.keys():
        data_dict[keys] = decrypt(data_dict[keys], key)

    return data_dict


def encrypt_database_row(
    database_id: int,
    data: dict[str, str],
    current_user: User,
) -> tuple[int, str]:
    database_id = data.get("database_id")
    table_id = data.get("table_id")
    rows_to_encrypt = data.get("rows_to_encrypt")
    update_database = data.get("update_database")

    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    get_database(database_id=database_id, current_user=current_user)

    # Create cloud database if not exist
    cloud_database_url = get_cloud_database_url(database_id=database_id)
    cloud_database_engine = create_engine(url=cloud_database_url)
    if not database_exists(url=cloud_database_engine.url):
        create_database(url=cloud_database_engine.url)

    # Load rsa keys
    database_keys = DatabaseKey.query.filter_by(database_id=database_id).first()

    if not database_keys:
        raise DefaultException("database_keys_not_found", code=404)

    public_key, _ = load_keys(
        publicKeyStr=database_keys.public_key,
        privateKeyStr=database_keys.private_key,
    )

    # Get columns to encrypt
    primary_key_name = get_primary_key_name(
        database_id=database_id, table_name=table.name
    )
    columns_list = [primary_key_name] + get_sensitive_columns(
        database_id=database_id, table_name=table.name
    )["sensitive_column_names"]

    # Create cloud table connection
    cloud_table_connection = create_table_connection(
        database_url=cloud_database_engine.url,
        table_name=table.name,
        columns_list=columns_list,
    )

    try:
        if update_database:
            for row in rows_to_encrypt:
                row = {key: row[key] for key in columns_list}

                primary_key_value = row[primary_key_name]
                row.pop(primary_key_name, None)

                row = encrypt_dict(data_dict=row, key=public_key)

                statement = (
                    update(cloud_table_connection.table)
                    .where(
                        cloud_table_connection.get_column(column_name=primary_key_name)
                        == primary_key_value
                    )
                    .values(row)
                )

                cloud_table_connection.session.execute(statement)

                cloud_table_connection.session.flush()
        else:
            for row in rows_to_encrypt:
                primary_key_value = row[primary_key_name]
                row.pop(primary_key_name, None)

                row = encrypt_dict(data_dict=row, key=public_key)

                row[primary_key_name] = primary_key_value

                statement = insert(cloud_table_connection.table).values(row)

                cloud_table_connection.session.execute(statement)

                cloud_table_connection.session.flush()

        cloud_table_connection.session.commit()
    except:
        cloud_table_connection.session.rollback()
        raise DefaultException("database_rows_not_encrypted", code=500)
    finally:
        cloud_table_connection.close()


def encrypt_database_table(data: dict[str, str], current_user: User) -> None:
    database_id = data.get("database_id")
    table_id = data.get("table_id")

    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    database = get_database(
        database_id=database_id, current_user=current_user, verify_connection=True
    )

    try:
        # Create cloud database if not exist
        cloud_database_engine = create_engine(
            url=get_cloud_database_url(database_id=database_id)
        )

        if database_exists(url=cloud_database_engine.url):
            drop_database(url=cloud_database_engine.url)
        create_database(url=cloud_database_engine.url)

        # Create client table connection
        client_table_connection = create_table_connection(
            database_url=database.url, table_name=table.name
        )

        # Get client table columns
        primary_key_name = get_primary_key_name(
            database_id=database_id, table_name=table.name
        )
        columns_list = [primary_key_name] + get_sensitive_columns(
            database_id=database_id, table_name=table.name
        )["sensitive_column_names"]

        # Load rsa keys
        database_keys = DatabaseKey.query.filter_by(database_id=database_id).first()

        if not database_keys:
            raise DefaultException("database_keys_not_found", code=404)

        public_key, _ = load_keys(
            publicKeyStr=database_keys.public_key,
            privateKeyStr=database_keys.private_key,
        )

        # Encrypt database
        results_proxy = client_table_connection.session.execute(
            select(client_table_connection.table)
        )  # Proxy to get data on batch

        results = results_proxy.fetchmany(
            _batch_selection_size
        )  # Getting rows database

        number_row_selected = 0

        number_row_total = (
            client_table_connection.session.query(func.count())
            .select_from(client_table_connection.table)
            .scalar()
        )
        print(f"Total = {number_row_total}")

        while results:
            from_db = [row._asdict() for row in results]

            # Create dataframe with data original database
            dataframe_db = pd.DataFrame(from_db, columns=columns_list)

            # Create name_columns without id
            names_columns = columns_list.copy()
            names_columns.remove(primary_key_name)

            # Encrypt each column
            for column in names_columns:
                dataframe_db[column] = encrypt_list(
                    list(dataframe_db[column]), public_key
                )

            # Add column hash
            dataframe_db["line_hash"] = [None] * len(dataframe_db)

            # Send data to database
            dataframe_db.to_sql(
                table.name,
                cloud_database_engine.connect(),
                if_exists="append",
                index=False,
            )

            number_row_selected += _batch_selection_size

            _calculate_progress(
                table=table,
                number_row_selected=number_row_selected,
                number_row_total=number_row_total,
            )

            time.sleep(5)
            print(f"Progress = {(number_row_selected // number_row_total ) * 100}")

            results = results_proxy.fetchmany(
                _batch_selection_size
            )  # Getting next rows database

        table.encryption_progress = 100
        db.session.commit()
    except:
        table.encryption_progress = 0
        db.session.commit()
        raise DefaultException("table_not_encrypted", code=500)
    finally:
        client_table_connection.close()
        cloud_database_engine.dispose()


def decrypt_row(
    data: dict[str, str],
    current_user: User = None,
) -> dict:
    database_id = data.get("database_id")
    table_id = data.get("table_id")
    search_type = data.get("search_type")
    search_value = data.get("search_value")

    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    client_database = get_database(database_id=database_id, current_user=current_user)

    # Get cloud database url
    cloud_database_url = get_cloud_database_url(database_id=database_id)

    # Create cloud table connection
    cloud_table_connection = create_table_connection(
        database_url=cloud_database_url, table_name=table.name
    )

    # Get primary key name of Client Database
    primary_key_name = get_primary_key_name(
        database_id=database_id, table_name=table.name
    )

    try:
        # Searching to row on Cloud Database
        if search_type == "primary_key":
            search_value = int(search_value)
            query_sensitive_data = (
                cloud_table_connection.session.query(cloud_table_connection.table)
                .filter(
                    cloud_table_connection.get_column(column_name=primary_key_name)
                    == search_value
                )
                .all()
            )
        elif search_type == "row_hash":
            query_sensitive_data = (
                cloud_table_connection.session.query(cloud_table_connection.table)
                .filter(
                    cloud_table_connection.get_column(column_name=primary_key_name)
                    == search_value
                )
                .all()
            )
        else:
            raise ValidationException(
                errors={"decryption": "search_invalid_data"},
                message="Input payload validation failed",
            )

        if not query_sensitive_data:
            raise DefaultException("cloud_row_not_found", code=404)

        # Transform sensitive data query to dictionary
        dict_sensitive_data = [row._asdict() for row in query_sensitive_data][0]

        # Remove primary key and hash value
        remove_primary_key_response = dict_sensitive_data.pop(primary_key_name, None)
        remove_line_hash_response = dict_sensitive_data.pop("line_hash", None)

        if remove_primary_key_response == None or remove_line_hash_response == None:
            raise DefaultException("internal_server_error", code=500)

        # Load encryption keys
        database_keys = DatabaseKey.query.filter_by(database_id=database_id).first()

        if not database_keys:
            raise DefaultException("database_keys_not_found", code=404)

        _, private_key = load_keys(
            publicKeyStr=database_keys.public_key,
            privateKeyStr=database_keys.private_key,
        )

        # Decrypt sensitive data
        dict_decrypted_sensitive_data = decrypt_dict(
            data_dict=dict_sensitive_data, key=private_key
        )

        # Create client table connection
        client_table_connection = create_table_connection(
            database_url=client_database.url, table_name=table.name
        )

        # Searching to row on Client Database
        query_non_sensitive_data = (
            client_table_connection.session.query(client_table_connection.table)
            .filter(
                client_table_connection.get_column(column_name=primary_key_name)
                == search_value
            )
            .all()
        )

        if not query_non_sensitive_data:
            raise DefaultException("client_row_not_found", code=404)

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
            database_id=database_id, table_name=table.name
        )

        for key in columns_types.keys():
            column_type = columns_types[key].split("(")[0]

            if column_type == "INTEGER":
                dict_result_data[key] = int(dict_result_data[key])
            elif column_type == "VARCHAR":
                dict_result_data[key] = str(dict_result_data[key])
            else:
                pass

        return dict_result_data
    except:
        raise DefaultException("row_not_decrypted", code=500)
