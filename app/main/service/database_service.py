import math

from sqlalchemy import and_, create_engine, inspect
from sqlalchemy.orm import joinedload
from sqlalchemy_utils import database_exists
from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException, ValidationException
from app.main.model import AnonymizationRecord, Database, DatabaseKey, User
from app.main.service.database_key_service import get_database_keys_by_database_id
from app.main.service.database_key_service import generate_keys
from app.main.service.valid_database_service import get_valid_database

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


def save_new_database(data: dict[str, str], current_user: User) -> None:
    name = data.get("name")
    username = data.get("username")
    host = data.get("host")
    port = data.get("port")
    valid_database = get_valid_database(valid_database_id=data.get("valid_database_id"))

    _validate_database_unique_constraint(
        name=name, username=username, host=host, port=port
    )

    try:
        new_database = Database(
            name=name,
            host=host,
            username=username,
            port=port,
            password=data.get("password"),
            valid_database=valid_database,
            user=current_user,
        )
        db.session.add(new_database)
        db.session.flush()
    except:
        db.session.rollback()
        raise DefaultException("database_not_added", code=500)

    try:
        # Generate rsa keys and save them
        public_key_string, private_key_string = generate_keys()
        database_keys = DatabaseKey(
            public_key=public_key_string,
            private_key=private_key_string,
            database=new_database,
        )
        db.session.add(database_keys)
    except:
        db.session.rollback()
        raise DefaultException("database_keys_not_added", code=500)

    # Commit updates
    db.session.commit()


def update_database(database_id: int, current_user: User, data: dict[str, str]) -> None:
    database = get_database(database_id=database_id)

    new_name = data.get("name")
    new_username = data.get("username")
    new_host = data.get("host")
    new_port = data.get("port")
    new_valid_database_id = data.get("valid_database_id")

    if (not current_user.is_admin) and (database.user_id != current_user.id):
        raise DefaultException("unauthorized_user", code=401)

    _validate_database_unique_constraint(
        name=new_name,
        username=new_username,
        host=new_host,
        port=new_port,
        filters=[Database.id != database_id],
    )

    if database.valid_database_id != new_valid_database_id:
        database.valid_database = get_valid_database(
            valid_database_id=new_valid_database_id
        )

    database.name = new_name
    database.host = new_host
    database.username = new_username
    database.port = new_port
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


def _validate_database_unique_constraint(
    name: str, username: str, host: str, port: int, filters: list = []
) -> None:
    if Database.query.filter(
        and_(
            Database.name == name,
            Database.username == username,
            Database.host == host,
            Database.port == port,
        ),
        *filters,
    ).first():
        raise DefaultException("database_already_exist", code=409)


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


def route_get_database_columns(
    database_id: int,
    current_user: User,
    params: ImmutableMultiDict,
) -> dict[list[str]]:
    table_name = params.get("table_name", type=str)

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
    columns_inspector = inspect(engine_database).get_columns(table_name)

    table_columns = []
    for column in columns_inspector:
        table_columns.append({"name": str(column["name"]), "type": str(column["type"])})

    return {"table_columns": table_columns}


def get_database_columns_types(
    database_id: int, table_name: str
) -> tuple[None, int, str] | tuple[dict[str, str], int, str]:
    # Get database url
    database_url = get_database_url(database_id=database_id)

    # Create connection to database
    engine_db = create_engine(database_url)

    # Get columns and their types
    columns = {}

    insp = inspect(engine_db)
    columns_table = insp.get_columns(table_name)

    for c in columns_table:
        columns[f"{c['name']}"] = str(c["type"])

    return columns


def get_sensitive_columns(database_id: int, table_name: str) -> dict:
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


def get_route_sensitive_columns(
    database_id: int,
    current_user: User,
    params: ImmutableMultiDict,
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


def test_database_connection(database_id: int, current_user: User) -> None:
    database = get_database(database_id=database_id)

    if database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    database_url = get_database_url(database_id=database_id)

    try:
        engine = create_engine(database_url)

        if not database_exists(engine.url):
            raise DefaultException("database_not_connected", code=409)
    except:
        raise DefaultException("database_not_connected", code=409)


def test_database_connection_by_url(data: dict[str, str]) -> None:
    valid_database = get_valid_database(valid_database_id=data.get("valid_database_id"))
    name = data.get("name")
    username = data.get("username")
    password = data.get("password")
    host = data.get("host")
    port = data.get("port")

    database_url = f"{valid_database.name}://{username}:{password}@{host}:{port}/{name}"

    try:
        engine = create_engine(database_url)

        if not database_exists(engine.url):
            raise DefaultException("database_not_connected", code=409)
    except:
        raise DefaultException("database_not_connected", code=409)
