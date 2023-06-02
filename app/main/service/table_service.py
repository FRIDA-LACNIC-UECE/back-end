import math

from sqlalchemy import and_
from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import Table, User
from app.main.service.database_service import get_database

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


def get_tables(
    params: ImmutableMultiDict, database_id: int, current_user: User
) -> dict:
    database = get_database(database_id=database_id)
    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    name = params.get("table_name", type=str)

    if database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    filters = [Table.database_id == database_id]

    if name:
        filters.append(Table.name.ilike(f"%{name}%"))

    pagination = (
        Table.query.filter(*filters)
        .order_by(Table.id)
        .paginate(page=page, per_page=per_page, error_out=False)
    )

    return {
        "current_page": page,
        "total_items": pagination.total,
        "total_pages": math.ceil(pagination.total / per_page),
        "items": pagination.items,
    }


def get_table_by_id(table_id: int, database_id: int, current_user: User):
    return get_table(
        table_id=table_id, database_id=database_id, current_user=current_user
    )


def save_new_table(data: dict, database_id: int, current_user: User) -> None:
    database = get_database(database_id=database_id)
    name = data.get("name")

    if database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    _validate_table_unique_constraint(table_name=name, database_id=database_id)

    new_Table = Table(name=name, database=database)
    db.session.add(new_Table)

    db.session.commit()


def update_table(
    table_id: int, database_id: int, data: dict[str, str], current_user: User
) -> None:
    table = get_table(
        table_id=table_id, database_id=database_id, current_user=current_user
    )

    name = data.get("name")

    if table.name != name:
        _validate_table_unique_constraint(
            table_name=name, database_id=database_id, filters=[Table.id != table_id]
        )
        table.name = name

    db.session.commit()


def delete_table(table_id: int, database_id: int, current_user: User) -> None:
    table = get_table(
        table_id=table_id, database_id=database_id, current_user=current_user
    )

    db.session.delete(table)
    db.session.commit()


def get_table(
    table_id: int,
    database_id: int,
    current_user: User,
    options: list = None,
) -> Table:
    database = get_database(database_id=database_id)

    if database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    query = Table.query

    filters = [Table.id == table_id, Table.database_id == database_id]

    if options is not None:
        query = query.options(*options)

    table = query.filter(*filters).first()

    if table is None:
        raise DefaultException("table_not_found", code=404)

    return table


def _validate_table_unique_constraint(
    table_name: str, database_id: int, filters: list = []
):
    if (
        Table.query.with_entities(Table.name, Table.database_id)
        .filter(
            Table.database_id == database_id,
            Table.name == table_name,
            *filters,
        )
        .first()
    ):
        raise DefaultException("table_already_exists", code=409)
