import math

from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import AnonymizationRecord, User
from app.main.service.anonymization_type_service import get_anonymization_type
from app.main.service.database_service import get_database

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


def get_anonymization_records(params: ImmutableMultiDict) -> dict:
    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    database_id = params.get("database_id", type=str)

    filters = []

    if database_id:
        filters.append(AnonymizationRecord.database_id == database_id)

    pagination = (
        AnonymizationRecord.query.filter(*filters)
        .order_by(AnonymizationRecord.id)
        .paginate(page=page, per_page=per_page, error_out=False)
    )
    total, anonymization_records = pagination.total, pagination.items

    return {
        "current_page": page,
        "total_items": pagination.total,
        "total_pages": math.ceil(total / per_page),
        "items": anonymization_records,
    }


def get_anonymization_records_by_id(
    anonymization_record_id: int,
) -> AnonymizationRecord:

    anonymization_record = AnonymizationRecord.query.filter(
        AnonymizationRecord.id == anonymization_record_id
    ).first()

    return anonymization_record


def get_anonymization_records_by_database_id(
    database_id: int,
) -> list[AnonymizationRecord]:

    anonymization_records = AnonymizationRecord.query.filter(
        AnonymizationRecord.database_id == database_id
    ).all()

    return anonymization_records


def save_new_anonymization_record(current_user: User, data: dict[str, str]) -> None:

    database = get_database(database_id=data.get("database_id"))

    if (not current_user.is_admin) and (database.user_id != current_user.id):
        raise DefaultException("unauthorized_user", code=401)

    anonymization_type = get_anonymization_type(
        anonymization_type_id=data.get("anonymization_type_id")
    )

    new_anonymization_record = AnonymizationRecord(
        table=data.get("table_name"),
        columns=data.get("columns"),
        database=database,
        anonymization_type=anonymization_type,
    )

    db.session.add(new_anonymization_record)
    db.session.commit()


def delete_anonymization_record_by_id(
    anonymization_record_id: int, current_user: User
) -> None:

    anonymization_record = get_anonymization_record(
        anonymization_record_id=anonymization_record_id
    )

    if (not current_user.is_admin) and (
        anonymization_record.database_id != current_user.id
    ):
        raise DefaultException("unauthorized_user", code=401)

    db.session.delete(anonymization_record)
    db.session.commit()


def delete_anonymization_records_by_database_id(
    database_id: int, current_user: User
) -> None:

    anonymization_records = get_anonymization_records_by_database_id(
        database_id=database_id
    )

    if (not current_user.is_admin) and (
        anonymization_records[0].database.user_id != current_user.id
    ):
        raise DefaultException("unauthorized_user", code=401)

    for anonymization_record in anonymization_records:
        db.session.delete(anonymization_record)
        db.session.flush()

    db.session.commit()


def get_anonymization_record(
    anonymization_record_id: int, options: list = None
) -> AnonymizationRecord:

    query = AnonymizationRecord.query

    if options is not None:
        query = query.options(*options)

    anonymization_record = query.get(anonymization_record_id)

    if anonymization_record is None:
        raise DefaultException("anonymization_record_not_found", code=404)

    return anonymization_record
