from math import ceil

from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import AnonymizationRecord, Database, Table, User
from app.main.service.anonymization_type_service import get_anonymization_type
from app.main.service.database_service import get_database
from app.main.service.table_service import get_table

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


def get_anonymization_records(
    params: ImmutableMultiDict, current_user: User
) -> dict[str, any]:
    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    database_id = params.get("database_id", type=int)

    filters = [AnonymizationRecord.table.has(Database.user_id == current_user.id)]

    if database_id is not None:
        filters.append(AnonymizationRecord.table.has(Database.id == database_id))

    pagination = (
        AnonymizationRecord.query.filter(*filters)
        .order_by(AnonymizationRecord.table_id)
        .paginate(page=page, per_page=per_page, error_out=False)
    )

    return {
        "current_page": page,
        "total_items": pagination.total,
        "total_pages": ceil(pagination.total / per_page),
        "items": pagination.items,
    }


def get_anonymization_records_by_id(
    anonymization_record_id: int,
) -> AnonymizationRecord:
    return get_anonymization_record(anonymization_record_id=anonymization_record_id)


def get_anonymization_records_by_database_id(
    database_id: int,
) -> list[AnonymizationRecord]:
    anonymization_records = AnonymizationRecord.query.filter(
        AnonymizationRecord.database_id == database_id
    ).all()

    return anonymization_records


def save_new_anonymization_record(data: dict[str, str], current_user: User) -> None:
    table = get_table(
        database_id=data.get("database_id"),
        table_id=data.get("table_id"),
        current_user=current_user,
    )

    anonymization_type = get_anonymization_type(
        anonymization_type_id=data.get("anonymization_type_id")
    )

    new_anonymization_record = AnonymizationRecord(
        columns=data.get("columns"),
        table=table,
        anonymization_type=anonymization_type,
    )

    db.session.add(new_anonymization_record)
    db.session.commit()


def update_anonymization_record(
    anonymization_record_id: int, data: dict[str, str], current_user: User
) -> None:
    anonymization_record = get_anonymization_record(
        anonymization_record_id=anonymization_record_id
    )

    if anonymization_record.table.database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    anonymization_record.columns = data.get("columns")

    db.session.commit()


def delete_anonymization_record_by_id(
    anonymization_record_id: int, current_user: User
) -> None:
    anonymization_record = get_anonymization_record(
        anonymization_record_id=anonymization_record_id
    )

    if anonymization_record.table.database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    db.session.delete(anonymization_record)
    db.session.commit()


def delete_anonymization_records_by_database_id(
    database_id: int, current_user: User
) -> None:
    anonymization_records = get_anonymization_records_by_database_id(
        database_id=database_id
    )

    if anonymization_records[0].database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    try:
        for anonymization_record in anonymization_records:
            db.session.delete(anonymization_record)
            db.session.flush()
    except:
        raise DefaultException("anonymization_record_not_deleted", code=500)

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
