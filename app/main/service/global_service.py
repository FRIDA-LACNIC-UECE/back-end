import os


from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.orm import Session

from app.main.config import Config
from app.main.service.database_service import (
    get_database,
    get_database_columns,
    get_database_url,
)


def get_cloud_database_url(database_id: int) -> str:
    database = get_database(database_id=database_id)

    env_name = os.environ.get("ENV_NAME", "dev")

    if env_name == "dev":
        return "{}://{}:{}@{}:{}/{}".format(
            "mysql",
            "root",
            "larces132",
            "localhost",
            "3306",
            f"{database.name}_cloud_U{database.user_id}DB{database.id}",
        )
    else:
        return "{}://{}:{}@{}:{}/{}".format(
            "mysql",
            "root",
            "",
            "db",
            "3306",
            f"{database.name}_cloud_U{database.user_id}DB{database.id}",
        )


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
