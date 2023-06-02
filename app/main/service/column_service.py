import math

from sqlalchemy import and_
from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import Column, Table, User
from app.main.service.database_service import get_database
from app.main.service.table_service import get_table

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


def get_columns(
    params: ImmutableMultiDict, database_id: int, table_id: int, current_user: User
) -> dict:
    database = get_database(database_id=database_id)
    table = get_table(
        table_id=table_id, database_id=database_id, current_user=current_user
    )
    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    name = params.get("column_name", type=str)
    anonymization_type_id = params.get("anonymization_type_id", type=int)

    if database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    filters = [Column.table_id == table_id]

    if name:
        filters.append(Column.name.ilike(f"%{name}%"))

    if table_id:
        filters.append(Column.table_id == table_id)

    if anonymization_type_id:
        filters.append(Column.anonimyzation_type_id == anonymization_type_id)

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


def save_new_column(
    data: dict, database_id: int, table_id: int, current_user: User
) -> None:
    database = get_database(database_id=database_id)
    table = get_table(
        table_id=table_id, database_id=database_id, current_user=current_user
    )
    anonymization_type_id = data.get("anonymization_type_id")
    type = data.get("type")
    name = data.get("name")

    if database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    _validate_column_unique_constraint(
        column_name=name, database_id=database_id, table_id=table_id
    )

    new_Column = Column(
        table=table,
        anonymization_type_id=anonymization_type_id,
        name=name,
        type=type,
    )
    db.session.add(new_Column)

    db.session.commit()


def update_column(
    column_id: int,
    table_id: int,
    database_id: int,
    data: dict[str, str],
    current_user: User,
) -> None:
    column = get_column(
        column_id=column_id,
        table_id=table_id,
        database_id=database_id,
        current_user=current_user,
    )

    name = data.get("name")
    anonimyzation_type_id = data.get("anonimyzation_type_id")

    if column.name != name:
        _validate_column_unique_constraint(
            column_name=name,
            database_id=database_id,
            table_id=table_id,
            filters=[Column.id != column_id],
        )
        column.name = name
        column.anonimyzation_type_id = anonimyzation_type_id

    db.session.commit()


def delete_column(
    column_id: int, table_id: int, database_id: int, current_user: User
) -> None:
    database = get_database(database_id=database_id)

    if database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    column = get_column(
        column_id=column_id,
        table_id=table_id,
        database_id=database_id,
        current_user=current_user,
    )

    db.session.delete(column)
    db.session.commit()


def get_column_by_id(
    column_id: int, table_id: int, database_id: int, current_user: User
):
    return get_column(
        column_id=column_id,
        table_id=table_id,
        database_id=database_id,
        current_user=current_user,
    )


def get_column(
    column_id: int,
    table_id: int,
    database_id: int,
    current_user: User,
    options: list = None,
) -> Column:
    database = get_database(database_id=database_id)

    if database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    query = Column.query

    filters = [Column.id == column_id, Column.table_id == table_id]

    if options is not None:
        query = query.options(*options)

    column = query.filter(*filters).first()

    if column is None:
        raise DefaultException("column_not_found", code=404)

    return column


def _validate_column_unique_constraint(
    column_name: str, database_id: int, table_id: int, filters: list = []
):
    if (
        Column.query.join(Table)
        .filter(
            Table.database_id == database_id,
            Table.id == table_id,
            Column.name == column_name,
            *filters,
        )
        .first()
    ):
        raise DefaultException("column_already_exists", code=409)
