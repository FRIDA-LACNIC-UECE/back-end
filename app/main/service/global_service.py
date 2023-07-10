import os

from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.orm import Session

from app.main.exceptions import DefaultException
from app.main.model.database_model import Database
from app.main.service.database_service import (
    get_database,
    get_database_columns_names_by_url,
    get_database_url,
)


class TableConnection:
    def __init__(self, engine, session, table):
        self.engine = engine
        self.session = session
        self.table_name = table.name
        self.table = table

    def get_column(self, column_name: str):
        for column in self.table.c:
            if column.name == column_name:
                return column
        return None

    def get_primary_key_name(self):
        return [key.name for key in inspect(self.table).primary_key][0]

    def close(self):
        self.session.close()
        self.engine.dispose()


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


def create_table_connection(
    database_url: str,
    table_name: str,
    columns_list: list = None,
) -> TableConnection:
    try:
        engine = create_engine(database_url)
        engine._metadata = MetaData(bind=engine)
        engine._metadata.reflect(engine)

        if columns_list == None:
            columns_list = get_database_columns_names_by_url(
                database_url=database_url, table_name=table_name
            )["column_names"]

        engine._metadata.tables[table_name].columns = [
            i
            for i in engine._metadata.tables[table_name].columns
            if (i.name in columns_list)
        ]

        # Create table object of Client Database
        table = Table(table_name, engine._metadata)

        # Create session of Client Database to run sql operations
        session = Session(engine)

        table_connection = TableConnection(engine=engine, session=session, table=table)
    except:
        raise DefaultException("internal_error_accessing_database", code=500)

    return table_connection


def get_primary_key_name(
    database_id: int = None, database_url: str = None, table_name: str = None
) -> None | str:
    if database_id:
        database_url = get_database_url(database_id=database_id)

    table_connection = create_table_connection(
        database_url=database_url, table_name=table_name
    )

    primary_key_name = [
        key.name for key in inspect(table_connection.table).primary_key
    ][0]

    table_connection.close()

    return primary_key_name
