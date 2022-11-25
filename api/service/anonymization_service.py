from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists

from config import HOST, PASSWORD_DATABASE, PORT, TYPE_DATABASE, USER_DATABASE
from model.anonymization_model import Anonymization, anonymizations_share_schema
from model.anonymization_type_model import AnonymizationType
from model.database_model import Database, database_share_schema
from service.anonymization_types import (
    anonypy_anonymization_service,
    date_anonymizer_service,
    email_anonymizer_service,
    ip_anonymizer_service,
    named_entities_anonymizer_service,
    ppcbtf_anonymization_service,
    ppcbti_anonymization_service,
)
from service.database_service import get_path_database
from service.sse_service import generate_hash_column


def anonymization_database_rows(
    id_db: int,
    table_name: str,
    rows_to_anonymization: list[dict],
    insert_database: bool,
) -> tuple:
    """
    This function anonymizes each the given rows according
    to pre-configured anonymizations.

    Parameters
    ----------
    id_db : int
        Database ID to be anonymized.

    table_name : str
        Table name where anonymization will be performed.

    rows_to_anonymization : list[dict]
        Rows provided for anonymization.

    insert_database : bool
        Flag to indicate if anonymized rows will be inserted or returned.

    Returns
    -------
    tuple
        If an error occurs, the return will be: (None, status code, response message).
        Else the return will be: (Anonymized rows, status code, response message).
    """

    # Check body request
    if (not id_db) or (not table_name) or (not rows_to_anonymization):
        return (None, 400, "anonymization_invalid_data")

    # Get client database path
    src_client_db_path = get_path_database(id_db)
    if not src_client_db_path:
        return (None, 404, "database_not_found")

    # Check connection with database found
    engine = create_engine(src_client_db_path)
    if not database_exists(engine.url):
        return (None, 409, "database_not_conected")

    # Get chosen columns
    lists_columns_anonymizations = anonymizations_share_schema.dump(
        Anonymization.query.filter_by(id_database=id_db, table=table_name).all()
    )
    if not lists_columns_anonymizations:
        return (None, 400, "anonymization_invalid_data")

    # Run anonymization for each anonymization types
    for anonymization in lists_columns_anonymizations:

        # Get anonymization type name by id
        anonymization_type_name = (
            AnonymizationType.query.filter_by(id=anonymization["id_anonymization_type"])
            .first()
            .name
        )

        # Run anonymization
        if anonymization_type_name == "named_entities_anonymizer":
            named_entities_anonymizer_service.anonymization_database_rows(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
                rows_to_anonymize=rows_to_anonymization,
                insert_database=insert_database,
            )
            print("\n named_entities_anonymizer anonimizando\n")
            print(rows_to_anonymization)

        elif anonymization_type_name == "date_anonymizer":
            date_anonymizer_service.anonymization_database_rows(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
                rows_to_anonymize=rows_to_anonymization,
                insert_database=insert_database,
            )
            print("\n date_anonymizer anonimizando\n")
            print(rows_to_anonymization)

        elif anonymization_type_name == "email_anonymizer":
            email_anonymizer_service.anonymization_database_rows(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
                rows_to_anonymize=rows_to_anonymization,
                insert_database=insert_database,
            )
            print("\n email_anonymizer anonimizando\n")
            print(rows_to_anonymization)

        elif anonymization_type_name == "ip_anonymizer":
            ip_anonymizer_service.anonymization_database_rows(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
                rows_to_anonymize=rows_to_anonymization,
                insert_database=insert_database,
            )
            print("\n ip_anonymizer anonimizando\n")
            print(rows_to_anonymization)

        else:
            return (None, 400, "anonymization_invalid_data")

    return (rows_to_anonymization, 200, "rows_database_anonymized")


def anonymization_table(src_client_db_path: str, id_db: int, table_name: str) -> int:
    """
    This function anonymizes all rows in a database table according
    to pre-configured anonymization.

    Parameters
    ----------
    src_client_db_path : str
        Connection URI of Client Database.

    id_db : int
        Database ID to be anonymized.

    table_name : str
        Table name where anonymization will be performed.

    Returns
    -------
    int
        status code.
    """

    # Get chosen columns
    lists_columns_anonymizations = anonymizations_share_schema.dump(
        Anonymization.query.filter_by(id_database=id_db, table=table_name).all()
    )
    if not lists_columns_anonymizations:
        return 400

    # Run anonymization for each anonymization types
    for anonymization in lists_columns_anonymizations:

        # Get anonymization type name by id
        anonymization_type_name = (
            AnonymizationType.query.filter_by(id=anonymization["id_anonymization_type"])
            .first()
            .name
        )

        # Run anonymization
        if anonymization_type_name == "named_entities_anonymizer":
            named_entities_anonymizer_service.anonymization_database(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
            )
            print("\n\n name_entities_anonymizer\n\n")

        elif anonymization_type_name == "date_anonymizer":
            date_anonymizer_service.anonymization_database(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
            )
            print("\n\n date_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "email_anonymizer":
            email_anonymizer_service.anonymization_database(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
            )
            print("\n\n date_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "ip_anonymizer":
            ip_anonymizer_service.anonymization_database(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
            )
            print("\n\n ip_anonymizer anonimizando\n\n")

        else:
            return 400

    return 200


def anonymization_database(id_db: int) -> tuple:
    """
    This function anonymizes all database rows in each table according
    to pre-configured anonymizations.

    Parameters
    ----------
    id_db : int
        Database ID to be anonymized.

    Returns
    -------
    tuple
        (status code, response message).
    """

    # Check request body
    if not id_db:
        return (400, "anonymization_invalid_data")

    # Get client database path
    src_client_db_path = get_path_database(id_db)

    if not src_client_db_path:
        return (404, "database_not_found")

    # Check connection with database found
    engine_client_db = create_engine(src_client_db_path)
    if not database_exists(engine_client_db.url):
        return (409, "database_not_conected")

    # Run anonymization for each table
    for table_name in list(engine_client_db.table_names()):
        status_code = anonymization_table(
            src_client_db_path=src_client_db_path, id_db=id_db, table_name=table_name
        )

        if status_code == 400:
            return (404, "database_not_found")

    # Get database path by id
    result_database = database_share_schema.dump(
        Database.query.filter_by(id=id_db).first()
    )

    # Get path of Cloud Database
    src_dest_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE,
        USER_DATABASE,
        PASSWORD_DATABASE,
        HOST,
        PORT,
        f"{result_database['name']}_cloud",
    )

    # Generate and insert hash on Cloud Database
    engine_cloud_db = create_engine(src_dest_db_path)

    for table_name in list(engine_cloud_db.table_names()):
        generate_hash_column(
            src_client_db_path=src_client_db_path,
            src_cloud_db_path=src_dest_db_path,
            src_table=table_name,
        )

    return (200, "database_anonymized")
