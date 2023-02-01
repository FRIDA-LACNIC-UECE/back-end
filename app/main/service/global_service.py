from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.orm import Session, joinedload

from app.main.config import Config
from app.main.exceptions import DefaultException, ValidationException
from app.main.model import AnonymizationRecord
from app.main.service.database_service import (
    get_database,
    get_database_columns,
    get_database_url,
)


def get_cloud_database_url(database_id: int) -> str:
    database = get_database(database_id=database_id)

    src_cloud_db_path = "{}://{}:{}@{}:{}/{}".format(
        Config.TYPE_DATABASE,
        Config.USER_DATABASE,
        Config.PASSWORD_DATABASE,
        Config.HOST,
        Config.PORT,
        f"{database.name}_cloud_U{database.user_id}DB{database.id}",
    )

    return src_cloud_db_path


def get_primary_key(
    database_id: int = None, database_url: str = None, table_name: str = None
) -> None | str:

    # Get database path by id
    if database_id:
        database_url = get_database_url(database_id=database_id)

    # Create table object of database
    table_object_db, _ = create_table_session(
        database_url=database_url, table_name=table_name
    )

    return [key.name for key in inspect(table_object_db).primary_key][0]


def get_sensitive_columns(database_id: int, table_name: str) -> list:

    # Get anonymization records
    anonymization_records = AnonymizationRecord.query.filter_by(
        database_id=database_id, table=table_name
    ).all()

    if not anonymization_records:
        raise ValidationException(
            errors={"anonymization_record": "anonymization_record_invalid_data"},
            message="Input payload validation failed",
        )

    sensitive_columns = []
    ids_type_anonymization = []

    # Get sensitive_columns
    for sensitive_column in anonymization_records:
        if sensitive_column.columns:
            sensitive_columns += sensitive_column.columns
            ids_type_anonymization += [sensitive_column.anonymization_type_id] * len(
                sensitive_column.columns
            )

    return sensitive_columns


def get_index_column_table_object(
    table_object: Table,
    column_name: list = None,
) -> None | int:

    # Search index of column
    index = 0
    for column in table_object.c:
        if column.name == column_name:
            return index
        index += 1

    return None


def create_table_session(
    database_id: int = None,
    database_url: str = None,
    table_name: str = None,
    columns_list: list = None,
) -> None | tuple[Table, Session]:

    # Get database path by id
    if database_id:
        database_url = get_database_url(database_id=database_id)

    # Create engine, reflect existing columns
    engine_db = create_engine(database_url)
    engine_db._metadata = MetaData(bind=engine_db)

    # Get columns from existing table
    engine_db._metadata.reflect(engine_db)

    if columns_list == None:
        columns_list = get_database_columns(
            database_id=database_id, database_url=database_url, table_name=table_name
        )["column_names"]

    engine_db._metadata.tables[table_name].columns = [
        i
        for i in engine_db._metadata.tables[table_name].columns
        if (i.name in columns_list)
    ]

    # Create table object of Client Database
    table_object_db = Table(table_name, engine_db._metadata)

    # Create session of Client Database to run sql operations
    session_db = Session(engine_db)

    return (table_object_db, session_db)
