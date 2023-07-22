import math

from sqlalchemy import and_
from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import Column, Database, Table, User
from app.main.service.anonymization_type_service import get_anonymization_type
from app.main.service.database_service import (
    get_database,
    get_database_columns_names_by_url,
    get_database_columns_types,
)
from app.main.service.table_service import get_table

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


def get_columns(
    params: ImmutableMultiDict, database_id: int, table_id: int, current_user: User
) -> dict:
    get_table(table_id=table_id, database_id=database_id, current_user=current_user)

    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    name = params.get("column_name", type=str)
    anonymization_type_id = params.get("anonymization_type_id", type=int)

    filters = [Column.table_id == table_id]

    if name:
        filters.append(Column.name.ilike(f"%{name}%"))

    if anonymization_type_id:
        filters.append(Column.anonymization_type_id == anonymization_type_id)

    pagination = (
        Column.query.filter(*filters)
        .order_by(Column.id)
        .paginate(page=page, per_page=per_page, error_out=False)
    )

    return {
        "current_page": page,
        "total_items": pagination.total,
        "total_pages": math.ceil(pagination.total / per_page),
        "items": pagination.items,
    }


def get_column_by_id(
    column_id: int, database_id: int, table_id: int, current_user: User
):
    return get_column(
        column_id=column_id,
        database_id=database_id,
        table_id=table_id,
        current_user=current_user,
    )


def save_new_column(
    data: dict, database_id: int, table_id: int, current_user: User
) -> None:
    database = get_database(database_id=database_id, current_user=current_user)
    table = get_table(
        table_id=table_id, database_id=database_id, current_user=current_user
    )
    anonymization_type = get_anonymization_type(
        anonymization_type_id=data.get("anonymization_type_id")
    )
    name = data.get("name")

    _validate_column_unique_constraint(
        column_name=name,
        database=database,
        table=table,
    )

    column_type = _get_column_type(
        database_id=database_id, table_name=table.name, column_name=name
    )

    new_column = Column(
        name=name,
        type=column_type,
        anonymization_type=anonymization_type,
        table=table,
    )

    db.session.add(new_column)
    db.session.commit()


def update_column(
    column_id: int,
    database_id: int,
    table_id: int,
    data: dict[str, str],
    current_user: User,
) -> None:
    column = get_column(
        column_id=column_id,
        table_id=table_id,
        database_id=database_id,
        current_user=current_user,
    )

    database = get_database(database_id=database_id, current_user=current_user)
    table = get_table(
        table_id=table_id, database_id=database_id, current_user=current_user
    )

    new_name = data.get("name")
    new_anonymization_type = get_anonymization_type(
        anonymization_type_id=data.get("anonymization_type_id")
    )

    if column.name != new_name:
        _validate_column_unique_constraint(
            column_name=new_name,
            database=database,
            table=table,
            filters=[Column.id != column_id],
        )

        column.name = new_name

    column_type = _get_column_type(
        database_id=database_id, table_name=table.name, column_name=new_name
    )

    column.type = column_type
    column.anonymization_type = new_anonymization_type

    db.session.commit()


def delete_column(
    column_id: int, table_id: int, database_id: int, current_user: User
) -> None:
    column = get_column(
        column_id=column_id,
        table_id=table_id,
        database_id=database_id,
        current_user=current_user,
    )

    db.session.delete(column)
    db.session.commit()


def get_column(
    column_id: int,
    table_id: int,
    database_id: int,
    current_user: User,
    options: list = None,
) -> Column:
    get_table(database_id=database_id, current_user=current_user)

    query = Column.query

    filters = [Column.id == column_id, Column.table_id == table_id]

    if options is not None:
        query = query.options(*options)

    column = query.filter(*filters).first()

    if column is None:
        raise DefaultException("column_not_found", code=404)

    return column


def _get_column_type(database_id: int, table_name: str, column_name: str) -> str:
    columns_types = get_database_columns_types(
        database_id=database_id, table_name=table_name
    )

    return columns_types[f"{column_name}"]


def _validate_column_unique_constraint(
    column_name: str,
    database: Database,
    table: Table,
    filters: list = [],
):
    database_columns_names = get_database_columns_names_by_url(
        database_url=database.url, table_name=table.name
    )["column_names"]

    if not column_name in database_columns_names:
        raise DefaultException("column_not_exists_on_client_database", code=409)

    if (
        Column.query.join(Table)
        .filter(
            Table.database_id == database.id,
            Table.id == table.id,
            Column.name == column_name,
            *filters,
        )
        .first()
    ):
        raise DefaultException("column_already_exists", code=409)
