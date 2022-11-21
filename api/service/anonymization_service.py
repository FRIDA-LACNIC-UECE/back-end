from config import HOST, PASSWORD_DATABASE, PORT, TYPE_DATABASE, USER_DATABASE
from model.anonymization_model import Anonymization, anonymizations_share_schema
from model.anonymization_type_model import AnonymizationType
from model.database_model import Database, database_share_schema
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists

from service.anonymization_types import (
    anonypy_anonymization_service,
    date_anonymization_service,
    ip_anonymization_service,
    name_anonymization_service,
    ppcbtf_anonymization_service,
    ppcbti_anonymization_service,
)
from service.database_service import get_path_database
from service.sse_service import generate_hash_column


def anonymization_one_database_row(
    id_db: int, table_name: str, row_to_anonymization: list
) -> tuple:

    # Check body request
    if (not id_db) or (not table_name) or (not row_to_anonymization):
        return (None, 400, "anonymization_invalid_data")

    # Get client database path
    src_client_db_path = get_path_database(id_db)
    if not src_client_db_path:
        return (None, 404, "database_not_found")

    # Check connection with database found
    engine_db = create_engine(src_client_db_path)
    if not database_exists(engine_db.url):
        return (None, 409, "database_not_conected")

    # Get chosen columns
    lists_columns_anonymizations = anonymizations_share_schema.dump(
        Anonymization.query.filter_by(id_database=id_db, table=table_name).all()
    )
    if not lists_columns_anonymizations:
        return (None, 400, "anonymization_invalid_data")

    # Run anonymization for each anonymization types
    for anonymization in lists_columns_anonymizations:

        response_anonymization = None

        # Get anonymization type name by id
        anonymization_type_name = (
            AnonymizationType.query.filter_by(id=anonymization["id_anonymization_type"])
            .first()
            .name
        )

        # Run anonymization
        if anonymization_type_name == "named_entities_anonymizer":
            response_anonymization = (
                name_anonymization_service.anonymization_one_database_row(
                    src_client_db_path=src_client_db_path,
                    table_name=anonymization["table"],
                    columns_to_anonymization=anonymization["columns"],
                    row_to_anonymization=row_to_anonymization,
                )
            )
            print("\n\n named entities anonymizer anonimizando\n\n")
        else:
            return (None, 400, "anonymization_invalid_data")

    return (row_to_anonymization, 200, "row_database_anonymized")


def anonymization_database_rows(
    id_db: int, table_name: str, rows_to_anonymization: list
) -> tuple:

    # Check body request
    if (not id_db) or (not table_name) or (not rows_to_anonymization):
        return (400, "anonymization_invalid_data")

    # Get client database path
    src_client_db_path = get_path_database(id_db)
    if not src_client_db_path:
        return (404, "database_not_found")

    # Check connection with database found
    engine = create_engine(src_client_db_path)
    if not database_exists(engine.url):
        return (409, "database_not_conected")

    # Get chosen columns
    lists_columns_anonymizations = anonymizations_share_schema.dump(
        Anonymization.query.filter_by(id_database=id_db, table=table_name).all()
    )
    if not lists_columns_anonymizations:
        return (400, "anonymization_invalid_data")

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
            name_anonymization_service.anonymization_database_rows(
                src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymization=anonymization["columns"],
                rows_to_anonymization=rows_to_anonymization,
            )
            print("\n\n ppcbtf anonimizando\n\n")

        elif anonymization_type_name == "ppcbtf":
            ppcbtf_anonymization_service.anonymization_database_rows(
                src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymization=anonymization["columns"],
                rows_to_anonymization=rows_to_anonymization,
            )
            print("\n\n ppcbtf anonimizando\n\n")

        elif anonymization_type_name == "ppcbti":
            ppcbti_anonymization_service.anonymization_database_rows(
                src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymization=anonymization["columns"],
                rows_to_anonymization=rows_to_anonymization,
            )
            print("\n\n ppcbti anonimizando\n\n")

        elif anonymization_type_name == "anonypy":
            anonypy_anonymization_service.anonymization_database_rows(
                src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymization=anonymization["columns"],
                rows_to_anonymization=rows_to_anonymization,
            )
            print("\n\n nonypy anonimizando\n\n")

        elif anonymization_type_name == "ip_anonymizer":
            ip_anonymization_service.anonymization_database_rows(
                src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymization=anonymization["columns"],
                rows_to_anonymization=rows_to_anonymization,
            )
            print("\n\n ip_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "date_anonymizer":
            date_anonymization_service.anonymization_database_rows(
                src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymization=anonymization["columns"],
                rows_to_anonymization=rows_to_anonymization,
            )
            print("\n\n date_anonymizer anonimizando\n\n")

        else:
            return (400, "anonymization_invalid_data")

    return (200, "rows_database_anonymized")


def anonymization_table(src_client_db_path: str, id_db: int, table_name: str) -> int:
    # Get chosen columns
    lists_columns_anonymizations = anonymizations_share_schema.dump(
        Anonymization.query.filter_by(id_database=id_db, table=table_name).all()
    )
    if not lists_columns_anonymizations:
        return (400, "anonymization_invalid_data")

    # Get each anonymization types
    for anonymization in lists_columns_anonymizations:

        # Get database path by id
        anonymization_type_name = (
            AnonymizationType.query.filter_by(id=anonymization["id_anonymization_type"])
            .first()
            .name
        )

        # Run anonymization
        if anonymization_type_name == "named_entities_anonymizer":
            name_anonymization_service.anonymization_database(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymization=anonymization["columns"],
            )
            print("\n\n name_entities_anonymizer\n\n")

        else:
            return 406


def anonymization_database(id_db: int) -> tuple:
    # Check body request
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
        anonymization_table(
            src_client_db_path=src_client_db_path, id_db=id_db, table_name=table_name
        )

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
