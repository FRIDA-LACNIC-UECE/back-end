import math

from sqlalchemy import and_
from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import Database, SqlLog, User
from app.main.service.database_service import get_database
from app.main.service.global_service import get_primary_key

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


def get_sql_logs(params: ImmutableMultiDict, current_user: User) -> dict:
    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    database_id = params.get("database_id", type=int)
    sql_command = params.get("sql_command", type=str)

    filters = []

    if sql_command:
        filters.append(SqlLog.sql_command.ilike(f"%{sql_command}%"))

    if database_id:
        filters.append(SqlLog.database_id == database_id)

    pagination = (
        SqlLog.query.join(Database)
        .filter(and_(Database.user_id == current_user.id, *filters))
        .order_by(SqlLog.id)
        .paginate(page=page, per_page=per_page, error_out=False)
    )
    total, sql_logs = pagination.total, pagination.items

    return {
        "current_page": page,
        "total_items": pagination.total,
        "total_pages": math.ceil(total / per_page),
        "items": sql_logs,
    }


def get_sql_log_by_id(sql_log_id: int):
    sql_log = get_sql_log(sql_log_id=sql_log_id)
    return sql_log


def save_new_sql_log(data: dict[str, str], current_user: User) -> None:

    database = get_database(database_id=data.get("database_id"))

    if database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    new_sql_log = SqlLog(
        database=database,
        sql_command=data.get("sql_command"),
    )

    db.session.add(new_sql_log)
    db.session.commit()


def update_sql_log(sql_log_id: int, data: dict[str, str]) -> None:
    database = get_database(database_id=data.get("database_id"))
    sql_log = get_sql_log(sql_log_id=sql_log_id)

    sql_log.database = database
    sql_log.sql_command = data.get("sql_command")

    db.session.commit()


def delete_sql_log(sql_log_id: int) -> None:
    sql_log = get_sql_log(sql_log_id=sql_log_id)

    db.session.delete(sql_log)
    db.session.commit()


def get_sql_log(sql_log_id: int, options: list = None) -> SqlLog:

    query = SqlLog.query

    if options is not None:
        query = query.options(*options)

    sql_log = query.get(sql_log_id)

    if sql_log is None:
        raise DefaultException("sql_log_not_found", code=404)

    return sql_log


def updates_log(database_id: int, table_name: str, data: dict):
    update_log = f"UPDATE {table_name} SET"

    for index in range(len(data["columns"])):
        update_log += f" {data['columns'][index]}='{data['values'][index]}'"

    primary_key_name = get_primary_key(database_id=database_id, table_name=table_name)

    update_log += f" WHERE {primary_key_name}={data['primary_key_value']}"

    new_sql_log = SqlLog(database_id=database_id, sql_command=update_log)

    db.session.add(new_sql_log)
    db.session.commit()
