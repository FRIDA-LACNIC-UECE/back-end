import math

from sqlalchemy import and_
from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import AnonymizationRecord, AnonymizationType

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE


def get_anonymization_types(params: ImmutableMultiDict) -> dict:
    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    name = params.get("name", type=str)

    filters = []

    if name:
        filters.append(AnonymizationType.name.ilike(f"%{name}%"))

    pagination = (
        AnonymizationType.query.filter(*filters)
        .order_by(AnonymizationType.id)
        .paginate(page=page, per_page=per_page, error_out=False)
    )
    total, anonymization_types = pagination.total, pagination.items

    return {
        "current_page": page,
        "total_items": pagination.total,
        "total_pages": math.ceil(total / per_page),
        "items": anonymization_types,
    }


def get_anonymization_type_by_id(anonymization_type_id: int) -> AnonymizationType:
    anonymization_type = get_anonymization_type(
        anonymization_type_id=anonymization_type_id
    )

    return anonymization_type


def save_new_anonymization_type(data: dict[str, str]) -> None:
    name = data.get("name")

    if _anonymization_type_exists(anonymization_type_name=name):
        raise DefaultException("anonymization_type_exists", code=409)

    new_anonymization_type = AnonymizationType(
        name=name,
    )

    db.session.add(new_anonymization_type)
    db.session.commit()


def update_anonymization_type(anonymization_type_id: int, data: dict[str, str]) -> None:
    anonymization_type = get_anonymization_type(
        anonymization_type_id=anonymization_type_id
    )

    new_name = data.get("name")

    if _anonymization_type_exists(anonymization_type_name=new_name):
        raise DefaultException("anonymization_type_exists", code=409)

    anonymization_type.name = new_name

    db.session.commit()


def delete_anonymization_type(anonymization_type_id: int) -> None:
    anonymization_type = get_anonymization_type(
        anonymization_type_id=anonymization_type_id
    )

    if AnonymizationRecord.query.filter(
        AnonymizationRecord.anonymization_type_id == anonymization_type_id
    ).first():
        raise DefaultException(
            "anonymization_records_with_anonymization_type_exists", code=409
        )

    db.session.delete(anonymization_type)
    db.session.commit()


def get_anonymization_type(
    anonymization_type_id: int, options: list = None
) -> AnonymizationType:

    query = AnonymizationType.query

    if options is not None:
        query = query.options(*options)

    anonymization_type = query.get(anonymization_type_id)

    if anonymization_type is None:
        raise DefaultException("anonymization_type_not_found", code=404)

    return anonymization_type


def _anonymization_type_exists(
    anonymization_type_name: str, anonymization_type_id: int = None
) -> bool:

    filters = []
    if anonymization_type_id:
        filters.append(AnonymizationType.id != anonymization_type_id)

    return (
        AnonymizationType.query.with_entities(AnonymizationType.id)
        .filter(
            and_(
                AnonymizationType.name == anonymization_type_name,
                *filters,
            )
        )
        .scalar()
        is not None
    )
