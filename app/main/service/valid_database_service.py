from math import ceil

from sqlalchemy import and_, or_
from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import Database, ValidDatabase

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE

_VALID_DATABASE_DEFAULT = [
    {"name": "mysql", "dialect": "mysql+mysqlconnector"},
    {"name": "postgresql", "dialect": "postgresql"},
]


def create_default_valid_database():
    for _valid_database in _VALID_DATABASE_DEFAULT:
        new_valid_database = ValidDatabase(
            name=_valid_database["name"], dialect=_valid_database["dialect"]
        )
        db.session.add(new_valid_database)
    db.session.commit()


def get_valid_databases(params: ImmutableMultiDict) -> dict[str, any]:
    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    name = params.get("name", type=str)

    filters = []

    if name is not None:
        filters.append(ValidDatabase.name.ilike(f"%{name}%"))

    pagination = (
        ValidDatabase.query.filter(*filters)
        .order_by(ValidDatabase.id)
        .paginate(page=page, per_page=per_page, error_out=False)
    )

    return {
        "current_page": page,
        "total_items": pagination.total,
        "total_pages": ceil(pagination.total / per_page),
        "items": pagination.items,
    }


def get_valid_database_by_id(valid_database_id: int) -> ValidDatabase:
    return get_valid_database(valid_database_id=valid_database_id)


def save_new_valid_database(data: dict[str, str]) -> None:
    name = data.get("name")
    dialect = data.get("dialect")

    _valid_database_exists(valid_database_name=name, valid_database_dialect=dialect)

    new_valid_database = ValidDatabase(name=name, dialect=dialect)

    db.session.add(new_valid_database)
    db.session.commit()


def update_valid_database(valid_database_id, data: dict[str, str]) -> None:
    valid_database = get_valid_database(valid_database_id=valid_database_id)

    new_name = data.get("name")
    new_dialect = data.get("dialect")

    _valid_database_exists(
        valid_database_name=new_name,
        valid_database_dialect=new_dialect,
        filters=[ValidDatabase.id != valid_database_id],
    )

    valid_database.name = new_name
    valid_database.dialect = new_dialect

    db.session.commit()


def delete_valid_database(valid_database_id: int) -> None:
    valid_database = get_valid_database(valid_database_id=valid_database_id)

    _verify_relationship(valid_database_id=valid_database_id)

    db.session.delete(valid_database)
    db.session.commit()


def get_valid_database(valid_database_id: int, options: list = None) -> ValidDatabase:
    query = ValidDatabase.query

    if options is not None:
        query = query.options(*options)

    valid_database = query.get(valid_database_id)

    if valid_database is None:
        raise DefaultException("valid_database_not_found", code=404)

    return valid_database


def _valid_database_exists(
    valid_database_name: str, valid_database_dialect: str, filters: list = []
) -> None:
    if (
        ValidDatabase.query.with_entities(ValidDatabase.id)
        .filter(
            and_(
                or_(
                    ValidDatabase.name.ilike(valid_database_name),
                    ValidDatabase.dialect.ilike(valid_database_dialect),
                ),
                *filters,
            )
        )
        .first()
    ):
        raise DefaultException("valid_database_exists", code=409)


def _verify_relationship(valid_database_id: int) -> None:
    if (
        Database.query.with_entities(Database.valid_database_id)
        .filter(Database.valid_database_id == valid_database_id)
        .first()
    ):
        raise DefaultException("valid_database_associated_with_database", code=409)
