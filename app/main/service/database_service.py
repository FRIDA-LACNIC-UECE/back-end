import math

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import joinedload
from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException, ValidationException
from app.main.model import AnonymizationRecord, Database, User
from app.main.service.database_key_service import get_database_keys_by_database_id

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


def get_databases(params: ImmutableMultiDict, current_user: User) -> dict:
    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    name = params.get("name", type=str)

    filters = []

    if name:
        filters.append(Database.name.ilike(f"%{name}%"))

    if not current_user.is_admin:
        filters.append(Database.user_id == current_user.id)

    pagination = (
        Database.query.filter(*filters)
        .order_by(Database.id)
        .paginate(page=page, per_page=per_page, error_out=False)
    )
    total, databases = pagination.total, pagination.items

    return {
        "current_page": page,
        "total_items": pagination.total,
        "total_pages": math.ceil(total / per_page),
        "items": databases,
    }


def get_database_by_id(database_id: int) -> Database:
    database = get_database(database_id=database_id)
    return database


def save_new_database(data: dict[str, str]) -> None:

    new_database = Database(
        user_id=data.get("user_id"),
        valid_database_id=data.get("valid_database_id"),
        name=data.get("name"),
        host=data.get("host"),
        username=data.get("username"),
        port=data.get("port"),
        password=data.get("password"),
    )

    db.session.add(new_database)
    db.session.commit()


def update_database(database_id: int, current_user: User, data: dict[str, str]) -> None:
    database = get_database(database_id=database_id)

    if (not current_user.is_admin) and (database.user_id != current_user.id):
        raise DefaultException("unauthorized_user", code=401)

    database.name = data.get("name")
    database.host = data.get("host")
    database.username = data.get("username")
    database.port = data.get("port")
    database.password = data.get("password")

    db.session.commit()


def delete_database(database_id: int, current_user: User) -> None:
    database = get_database(database_id=database_id)

    if (not current_user.is_admin) and (database.user_id != current_user.id):
        raise DefaultException("unauthorized_user", code=401)

    database_keys = get_database_keys_by_database_id(database_id=database_id)

    if database_keys:
        db.session.delete(database_keys)
        db.session.flush()

    anonymization_records = AnonymizationRecord.query.filter(
        AnonymizationRecord.database_id == database_id
    ).all()

    if anonymization_records:
        for anonymization_record in anonymization_records:
            db.session.delete(anonymization_record)
            db.session.flush()

    db.session.delete(database)
    db.session.commit()


def get_database(database_id: int, options: list = None) -> Database:

    query = Database.query

    if options is not None:
        query = query.options(*options)

    database = query.get(database_id)

    if database is None:
        raise DefaultException("database_not_found", code=404)

    return database


def get_database_url(database_id: int) -> str:

    database = get_database(
        database_id=database_id, options=[joinedload("valid_database")]
    )

    database_url = "{}://{}:{}@{}:{}/{}".format(
        database.valid_database.name,
        database.username,
        database.password,
        database.host,
        database.port,
        database.name,
    )

    return database_url


def get_database_tables(database_id: int, current_user: User) -> dict[list[str]]:

    database = get_database(database_id=database_id)

    database_url = get_database_url(database_id=database_id)

    if database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)
    try:
        engine_database = create_engine(database_url)
    except:
        raise ValidationException(
            errors={"database": "database_invalid_data"},
            message="Input payload validation failed",
        )

    tables_names = list(engine_database.table_names())

    return {"table_names": tables_names}


def get_database_columns(
    database_id: int = None,
    database_url: str = None,
    table_name: str = None,
    current_user: User = None,
) -> dict[list[str]]:

    if database_id:
        database_url = get_database_url(database_id=database_id)

    if current_user and database_id:
        database = get_database(database_id=database_id)
        if database.user_id != current_user.id:
            raise DefaultException("unauthorized_user", code=401)

    try:
        engine_database = create_engine(database_url)
    except:
        raise ValidationException(
            errors={"database": "database_invalid_data"},
            message="Input payload validation failed",
        )

    # Get database columns
    columns_names = []

    columns_table = inspect(engine_database).get_columns(table_name)

    for column_name in columns_table:
        columns_names.append(str(column_name["name"]))

    return {"column_names": columns_names}


def get_sensitive_columns(
    params: ImmutableMultiDict, database_id: int, current_user: User
) -> dict:

    table_name = params.get("table_name", type=str)

    client_database = get_database(database_id=database_id)

    # Check user authorization
    if client_database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    anonymization_records = AnonymizationRecord.query.filter(
        AnonymizationRecord.database_id == database_id,
        AnonymizationRecord.table == table_name,
    ).all()

    if not anonymization_records:
        raise ValidationException(
            errors={"database": "database_invalid_data"},
            message="Input payload validation failed",
        )

    sensitive_columns = []

    # Get sensitive_columns
    for sensitive_column in anonymization_records:
        if sensitive_column.columns:
            sensitive_columns += sensitive_column.columns

    return {"sensitive_column_names": sensitive_columns}
