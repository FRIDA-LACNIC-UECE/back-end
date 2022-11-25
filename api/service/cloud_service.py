from config import HOST, PASSWORD_DATABASE, PORT, TYPE_DATABASE, USER_DATABASE
from model.database_key_model import DatabaseKey, database_key_share_schema
from model.database_model import Database, database_share_schema
from service.database_service import (
    create_table_session,
    get_columns_database_types,
    get_index_column_table_object,
    get_path_database,
    get_primary_key,
)
from service.rsa_service import decrypt_dict, loadKeys


def row_search(
    id_db_user: int, id_db: int, table_name: str, search_type: str, search_value: any
) -> dict:
    """
    This function returns a row given with your decrypted sensitive
    data along with the non-sensitive data. The search for the row
    is done through a given valueof the primary key or the hash of
    the row.

    Parameters
    ----------
    id_db_user : int
        Database User ID.

    id_db : int
        ID of the database where the row will be searched.

    table_name : str
        Name of the table where the row to be searched is located.

    search_type : str
        Indicates the type of value used to search for the row
        ("primary key" or "hash_line").

    search_value : int or str
        If the row search uses the primary key value then the
        search_value will be an integer representing the primary
        key value. If the search is by hash, then the search_value
        will be a string with the hash value.

    Returns
    -------
    tuple
        (status code, response message).
    """

    # Get client database path
    src_client_db_path = get_path_database(id_db)
    if not src_client_db_path:
        return (None, 404, "database_not_found")

    # Return error: database does not belong to the user (401)
    result_database = Database.query.filter_by(id=id_db).first()
    if result_database.id_user != id_db_user:
        return (None, 401, "user_unauthorized")

    # Get path of Cloud Database
    result_database = database_share_schema.dump(result_database)
    src_cloud_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE,
        USER_DATABASE,
        PASSWORD_DATABASE,
        HOST,
        PORT,
        f"{result_database['name']}_cloud",
    )

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_db, session_cloud_db = create_table_session(
        src_db_path=src_cloud_db_path, table_name=table_name
    )

    # Get primary key name of Client Database
    primary_key_name = get_primary_key(src_client_db_path, table_name)

    # Searching to row on Cloud Database
    if search_type == "primary_key":
        index_column_to_search = get_index_column_table_object(
            src_db_path=src_cloud_db_path,
            table_name=table_name,
            column_name=primary_key_name,
        )
        query_sensitive_data = (
            session_cloud_db.query(table_cloud_db)
            .filter(table_cloud_db.c[index_column_to_search] == search_value)
            .all()
        )
    elif search_type == "hash":
        index_column_to_search = get_index_column_table_object(
            src_db_path=src_cloud_db_path,
            table_name=table_name,
            column_name="line_hash",
        )
        print(index_column_to_search)
        query_sensitive_data = (
            session_cloud_db.query(table_cloud_db)
            .filter(table_cloud_db.c[index_column_to_search] == search_value)
            .all()
        )
    else:
        return (None, 400, "search_invalid_data")

    if not query_sensitive_data:
        return (None, 404, "row_not_found")

    # Transform sensitive data query to dictionary
    dict_sensitive_data = [row._asdict() for row in query_sensitive_data][0]

    # Remove primary key and hash value
    primary_key_value = dict_sensitive_data[f"{primary_key_name}"]
    hash_value = dict_sensitive_data["line_hash"]
    remove_primary_key_response = dict_sensitive_data.pop(primary_key_name, None)
    remove_line_hash_response = dict_sensitive_data.pop("line_hash", None)

    if remove_primary_key_response == None or remove_line_hash_response == None:
        return (None, 500, "internal_server_error")

    # Load rsa keys
    result_keys = database_key_share_schema.dump(
        DatabaseKey.query.filter_by(id_db=id_db).first()
    )

    if not result_keys:
        return (None, 500, "database_keys_not_foundr")

    _, private_key = loadKeys(
        publicKeyStr=result_keys["public_key"], privateKeyStr=result_keys["private_key"]
    )

    # Decrypt sensitive
    dict_decrypted_sensitive_data = decrypt_dict(dict_sensitive_data, private_key)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, table_name=table_name
    )

    # Searching to row on Client Database
    index_column_to_search = get_index_column_table_object(
        src_db_path=src_client_db_path,
        table_name=table_name,
        column_name=primary_key_name,
    )
    query_non_sensitive_data = (
        session_client_db.query(table_client_db)
        .filter(table_client_db.c[index_column_to_search] == search_value)
        .all()
    )

    if not query_non_sensitive_data:
        return (None, 404, "row_not_found")

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
    columns_types = get_columns_database_types(id_db=id_db, table_name=table_name)[0]
    print(dict_result_data)
    print(columns_types)

    for key in columns_types.keys():
        column_type = columns_types[key].split("(")[0]

        if column_type == "INTEGER":
            dict_result_data[key] = int(dict_result_data[key])
        elif column_type == "VARCHAR":
            dict_result_data[key] = str(dict_result_data[key])
        else:
            pass

    return (dict_result_data, 200, "row_found")
