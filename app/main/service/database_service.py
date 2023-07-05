from math import ceil

from sqlalchemy import and_, create_engine, inspect
from sqlalchemy.orm import joinedload
from sqlalchemy_utils import database_exists
from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException, ValidationException
from app.main.model import (
    AnonymizationRecord,
    Database,
    DatabaseKey,
    SqlLog,
    Table,
    User,
)
from app.main.service.database_key_service import (
    generate_keys,
    get_database_keys_by_database_id,
)
from app.main.service.valid_database_service import get_valid_database

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


def get_databases(params: ImmutableMultiDict, current_user: User) -> dict[str, any]:
    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    name = params.get("name", type=str)
    valid_database_id = params.get("valid_database_id", type=int)
    username = params.get("name", type=str)

    filters = []

    if valid_database_id is not None:
        filters.append(Database.valid_database_id == valid_database_id)

    if name is not None:
        filters.append(Database.name.ilike(f"%{name}%"))

    if current_user.is_admin and username is not None:
        filters.append(Database.user.has(User.username.ilike(f"%{username}%")))

    if not current_user.is_admin:
        filters.append(Database.user_id == current_user.id)

    pagination = (
        Database.query.filter(*filters)
        .order_by(Database.id)
        .paginate(page=page, per_page=per_page, error_out=False)
    )

    return {
        "current_page": page,
        "total_items": pagination.total,
        "total_pages": ceil(pagination.total / per_page),
        "items": pagination.items,
    }


def get_database_by_id(database_id: int, current_user: User) -> Database:
    return get_database(
        database_id=database_id, current_user=current_user, admin_permission=True
    )


def save_new_database(data: dict[str, str], current_user: User) -> None:
    valid_database = get_valid_database(valid_database_id=data.get("valid_database_id"))
    name = data.get("name")
    username = data.get("username")
    host = data.get("host")
    port = data.get("port")

    _validate_database_unique_constraint(
        valid_database_id=valid_database.id,
        name=name,
        username=username,
        host=host,
        port=port,
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
        raise DefaultException("database_not_created", code=500)

    try:
        public_key_string, private_key_string = generate_keys()
        database_keys = DatabaseKey(
            public_key=public_key_string,
            private_key=private_key_string,
            database=new_database,
        )
        db.session.add(database_keys)
    except:
        db.session.rollback()
        raise DefaultException("database_not_created", code=500)

    db.session.commit()


def update_database(database_id: int, current_user: User, data: dict[str, str]) -> None:
    database = get_database(
        database_id=database_id, current_user=current_user, admin_permission=True
    )

    new_name = data.get("name")
    new_username = data.get("username")
    new_host = data.get("host")
    new_port = data.get("port")
    new_valid_database_id = data.get("valid_database_id")

    _validate_database_unique_constraint(
        valid_database_id=new_valid_database_id,
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
    database = get_database(
        database_id=database_id, current_user=current_user, admin_permission=True
    )

    try:
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

        sql_logs = SqlLog.query.filter(SqlLog.database_id == database_id).all()

        if sql_logs:
            for sql_log in sql_logs:
                db.session.delete(sql_log)
                db.session.flush()

        tables = Table.query.filter(Table.database_id == database_id).all()

        if tables:
            for table in tables:
                db.session.delete(table)
                db.session.flush()
    except:
        db.session.rollback()
        raise DefaultException("database_not_deleted", code=500)

    db.session.delete(database)
    db.session.commit()


def get_database(
    database_id: int,
    current_user: User = None,
    admin_permission: bool = False,
    verify_connection: bool = False,
    options: list = None,
) -> Database:
    query = Database.query

    if options is not None:
        query = query.options(*options)

    database = query.get(database_id)

    if database is None:
        raise DefaultException("database_not_found", code=404)

    if verify_connection:
        if not database_exists(url=database.url):
            raise DefaultException("database_not_conected", code=409)

    if admin_permission and current_user.is_admin:
        return database

    if current_user is not None:
        if database.user_id != current_user.id:
            raise DefaultException("unauthorized_user", code=401)

    return database


def _validate_database_unique_constraint(
    valid_database_id: int,
    name: str,
    username: str,
    host: str,
    port: int,
    filters: list = [],
) -> None:
    if Database.query.filter(
        and_(
            Database.valid_database_id == valid_database_id,
            Database.name == name,
            Database.username == username,
            Database.host == host,
            Database.port == port,
        ),
        *filters,
    ).first():
        raise DefaultException("database_already_exists", code=409)


def get_database_url(database_id: int) -> str:
    database = get_database(
        database_id=database_id, options=[joinedload("valid_database")]
    )

    return database.url


def get_database_tables_names(
    database_id: int, current_user: User
) -> dict[str, list[str]]:
    database = get_database(
        database_id=database_id, current_user=current_user, verify_connection=True
    )

    try:
        engine_database = create_engine(database.url)
        tables_names = list(engine_database.table_names())
        return {"table_names": tables_names}
    except:
        raise DefaultException("internal_error_getting_tables_names", code=500)


def get_database_tables_names_by_url(database_url: str) -> dict[str, list[str]]:
    try:
        engine_database = create_engine(database_url)
        tables_names = list(engine_database.table_names())
        return {"table_names": tables_names}
    except:
        raise DefaultException("internal_error_getting_tables_names", code=500)


def get_database_columns(
    database_id: int,
    current_user: User,
    params: ImmutableMultiDict,
) -> dict[str, list[str]]:
    database = get_database(
        database_id=database_id, current_user=current_user, verify_connection=True
    )
    database_tables_names = get_database_tables_names(
        database_id=database_id, current_user=current_user
    )
    table_name = params.get("table_name", type=str)

    if not table_name in database_tables_names["table_names"]:
        raise DefaultException("table_not_found", code=404)

    try:
        engine_database = create_engine(database.url)

        columns_inspector = inspect(engine_database).get_columns(table_name)

        table_columns = []
        for column in columns_inspector:
            table_columns.append(
                {"name": str(column["name"]), "type": str(column["type"])}
            )

        return {"table_columns": table_columns}
    except:
        raise DefaultException("internal_error_getting_table_columns", code=500)


def get_database_columns_names_by_url(
    database_url: str, table_name: str
) -> dict[str, list[str]]:
    database_tables_names = get_database_tables_names_by_url(database_url=database_url)

    if not table_name in database_tables_names["table_names"]:
        raise DefaultException("table_not_found", code=404)

    try:
        engine_database = create_engine(database_url)

        columns_table = inspect(engine_database).get_columns(table_name)

        columns_names = []
        for column_name in columns_table:
            columns_names.append(str(column_name["name"]))

        return {"column_names": columns_names}
    except:
        raise DefaultException("internal_error_getting_table_columns", code=500)


def get_database_columns_types(database_id: int, table_name: str) -> dict[str, str]:
    database = get_database(
        database_id=database_id,
        current_user=None,
        admin_permission=False,
        verify_connection=False,
    )

    try:
        engine_db = create_engine(database.url)

        inspector = inspect(engine_db)
        columns_table = inspector.get_columns(table_name)

        columns = {}
        for column in columns_table:
            columns[f"{column['name']}"] = str(column["type"])

        return columns
    except:
        raise DefaultException("internal_error_getting_columns_types", code=500)



def test_database_connection(database_id: int, current_user: User) -> None:
    database = get_database(database_id=database_id, current_user=current_user)

    try:
        engine = create_engine(database.url)

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
