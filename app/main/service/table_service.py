import math

from sqlalchemy import create_engine, inspect
from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import AnonymizationRecord, Table, User

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


def get_tables(
    database_id: int, params: ImmutableMultiDict, current_user: User
) -> dict[str, any]:
    get_database(database_id=database_id, current_user=current_user)

    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    name = params.get("table_name", type=str)

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


def get_table_by_id(table_id: int, database_id: int, current_user: User) -> Table:
    return get_table(
        table_id=table_id, database_id=database_id, current_user=current_user
    )


def save_new_table(database_id: int, data: dict[str, str], current_user: User) -> None:
    database = get_database(database_id=database_id, current_user=current_user)
    name = data.get("name")

    _validate_table_unique_constraint(
        table_name=name, database_id=database_id, current_user=current_user
    )

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
            table_name=name,
            database_id=database_id,
            current_user=current_user,
            filters=[Table.id != table_id],
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
    database_id: int,
    table_id: int,
    current_user: User,
    options: list = None,
) -> Table:
    get_database(database_id=database_id, current_user=current_user)

    query = Table.query

    filters = [Table.id == table_id, Table.database_id == database_id]

    if options is not None:
        query = query.options(*options)

    table = query.filter(*filters).first()

    if table is None:
        raise DefaultException("table_not_found", code=404)

    return table


def _validate_table_unique_constraint(
    table_name: str, database_id: int, current_user: User, filters: list = []
):
    database_tables_names = get_database_tables_names(
        database_id=database_id, current_user=current_user
    )["table_names"]

    if not table_name in database_tables_names:
        raise DefaultException("table_not_exists_on_client_database", code=409)

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


def get_database_columns(
    database_id: int,
    table_id: int,
    current_user: User,
) -> dict[str, list[str]]:
    database = get_database(
        database_id=database_id, current_user=current_user, verify_connection=True
    )

    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    database_tables_names = get_database_tables_names(
        database_id=database_id, current_user=current_user
    )

    if not table.name in database_tables_names["table_names"]:
        raise DefaultException("outdated_table", code=409)

    try:
        engine_database = create_engine(database.url)

        columns_inspector = inspect(engine_database).get_columns(table.name)

        table_columns = []
        for column in columns_inspector:
            table_columns.append(
                {"name": str(column["name"]), "type": str(column["type"])}
            )

        return {"table_columns": table_columns}
    except:
        raise DefaultException("internal_error_getting_table_columns", code=500)


def get_sensitive_columns(
    database_id: int, table_id: int, current_user: User = None
) -> dict[str, list[str]]:
    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    database_tables_names = get_database_tables_names(
        database_id=database_id, current_user=current_user
    )

    if not table.name in database_tables_names["table_names"]:
        raise DefaultException("outdated_table", code=409)

    try:
        anonymization_records = AnonymizationRecord.query.filter(
            AnonymizationRecord.table_id == table.id,
        ).all()

        if not anonymization_records:
            raise DefaultException("anonymization_records_not_found", code=404)

        sensitive_columns = []
        for sensitive_column in anonymization_records:
            if sensitive_column.columns:
                sensitive_columns += sensitive_column.columns

        return {"sensitive_column_names": sensitive_columns}
    except:
        raise DefaultException(
            "internal_error_getting_sensitive_column_names", code=500
        )


def get_route_sensitive_columns(
    database_id: int,
    table_id: int,
    current_user: User,
) -> dict[str, list[str]]:
    get_database(
        database_id=database_id, current_user=current_user, verify_connection=True
    )

    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    database_tables_names = get_database_tables_names(
        database_id=database_id, current_user=current_user
    )

    if not table.name in database_tables_names["table_names"]:
        raise DefaultException("outdated_table", code=409)

    try:
        anonymization_records = AnonymizationRecord.query.filter(
            AnonymizationRecord.table_id == table.id,
        ).all()

        if not anonymization_records:
            raise DefaultException("anonymization_records_not_found", code=404)

        sensitive_columns = []
        for sensitive_column in anonymization_records:
            if sensitive_column.columns:
                sensitive_columns += sensitive_column.columns

        return {"sensitive_column_names": sensitive_columns}
    except:
        raise DefaultException(
            "internal_error_getting_sensitive_column_names", code=500
        )


from app.main.service.database_service import get_database, get_database_tables_names
from app.main.service.table_service import get_table
