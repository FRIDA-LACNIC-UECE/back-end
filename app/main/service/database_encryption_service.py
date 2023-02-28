import base64

import pandas as pd
import rsa
from sqlalchemy import create_engine, select
from sqlalchemy_utils import create_database, database_exists

from app.main.exceptions import DefaultException
from app.main.model import DatabaseKey
from app.main.service.database_service import get_database, get_sensitive_columns
from app.main.service.global_service import (
    create_table_session,
    get_cloud_database_url,
    get_primary_key,
)

"""from .database_service import (
    create_table_session,
    get_cloud_database_path,
    get_database_path,
    get_database_user_id,
    get_primary_key,
    get_sensitive_columns,
)"""


def generate_keys():
    # Generating keys
    (public_key, private_key) = rsa.newkeys(2048)

    # Save in PEM format
    public_key_PEM = public_key.save_pkcs1("PEM")
    private_key_PEM = private_key.save_pkcs1("PEM")

    # Transform from PEM to string base64
    public_key_str = str(base64.b64encode(public_key_PEM))[2:-1]
    private_key_str = str(base64.b64encode(private_key_PEM))[2:-1]

    return public_key_str, private_key_str


def load_keys(public_key_str, private_key_str):
    # Transform from string base64 to Byte
    public_key_byte = base64.b64decode(public_key_str.encode())
    private_key_byte = base64.b64decode(private_key_str.encode())

    public_key = rsa.PublicKey.load_pkcs1(public_key_byte)
    private_key = rsa.PrivateKey.load_pkcs1(private_key_byte)

    return public_key, private_key


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


def encrypt_database(data: dict[str, str]) -> None:
    user_id = data.get("user_id")
    database_id = data.get("database_id")
    table_name = data.get("table_name")

    # Get client database url
    database = get_database(database_id=database_id)

    if database.user_id != user_id:
        raise DefaultException("unauthorized_user", code=401)

    # Create cloud database if not exist
    engine_cloud_db = create_engine(url=get_cloud_database_url(database_id=database_id))
    if not database_exists(url=engine_cloud_db.url):
        create_database(engine_cloud_db.url)

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        database_id=database_id, table_name=table_name
    )

    # Get columns to encrypt
    primary_key_name = get_primary_key(database_id=database_id, table_name=table_name)
    columns_list = [primary_key_name] + get_sensitive_columns(
        database_id=database_id, table_name=table_name
    )["sensitive_columns"]

    # Get public and private keys of database
    database_keys = DatabaseKey.query.filter_by(database_id=database_id).first()

    if not database_keys:
        raise DefaultException("database_keys_not_found", code=404)

    # Load rsa keys
    public_key, _ = load_keys(
        public_key_str=database_keys.public_key,
        private_key_str=database_keys.private_key,
    )

    size_batch = 100
    statement = select(table_client_db)
    results_proxy = session_client_db.execute(statement)  # Proxy to get data on batch
    results = results_proxy.fetchmany(size_batch)  # Getting data

    # Encrypt database
    while results:

        from_db = [row._asdict() for row in results]

        # Create dataframe with data original database
        dataframe_db = pd.DataFrame(from_db, columns=columns_list)

        # Create name_columns without id
        names_columns = columns_list.copy()
        names_columns.remove(primary_key_name)

        # Encrypt each column
        for column in names_columns:
            dataframe_db[column] = encrypt_list(list(dataframe_db[column]), public_key)

        # Add column hash
        dataframe_db["line_hash"] = [None] * len(dataframe_db)

        # Send data to database
        dataframe_db.to_sql(
            table_name, engine_cloud_db.connect(), if_exists="append", index=False
        )

        results = results_proxy.fetchmany(size_batch)  # Getting next data

    return None
