from math import ceil

from sqlalchemy import and_
from werkzeug.datastructures import ImmutableMultiDict

from app.main import db
from app.main.config import Config
from app.main.exceptions import DefaultException
from app.main.model import AnonymizationRecord, AnonymizationType

_DEFAULT_CONTENT_PER_PAGE = Config.DEFAULT_CONTENT_PER_PAGE

_ANONYMIZATION_TYPE_DEFAULT = [
    "cpf_anonymizer",
    "date_anonymizer",
    "email_anonymizer",
    "ip_anonymizer",
    "named_entities_anonymizer",
    "rg_anonymizer",
    "date_cryptopan_anonymizer",
]


def create_default_anonymization_type():
    for anonymization_type_name in _ANONYMIZATION_TYPE_DEFAULT:
        new_anonymization_type = AnonymizationType(name=anonymization_type_name)
        db.session.add(new_anonymization_type)
    db.session.commit()


def get_anonymization_types(params: ImmutableMultiDict) -> dict[str, any]:
    page = params.get("page", type=int, default=1)
    per_page = params.get("per_page", type=int, default=_DEFAULT_CONTENT_PER_PAGE)
    name = params.get("name", type=str)

    filters = []

    if name is not None:
        filters.append(AnonymizationType.name.ilike(f"%{name}%"))

    pagination = (
        AnonymizationType.query.filter(*filters)
        .order_by(AnonymizationType.id)
        .paginate(page=page, per_page=per_page, error_out=False)
    )

    return {
        "current_page": page,
        "total_items": pagination.total,
        "total_pages": ceil(pagination.total / per_page),
        "items": pagination.items,
    }


def get_anonymization_type_by_id(anonymization_type_id: int) -> AnonymizationType:
    return get_anonymization_type(anonymization_type_id=anonymization_type_id)


def save_new_anonymization_type(data: dict[str, str]) -> None:
    name = data.get("name")

    _anonymization_type_exists(anonymization_type_name=name)

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

    if anonymization_type.name != new_name:
        _anonymization_type_exists(anonymization_type_name=new_name)

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
    anonymization_type_name: str, filters: list = []
) -> bool:
    if (
        AnonymizationType.query.with_entities(AnonymizationType.id)
        .filter(
            and_(
                AnonymizationType.name.ilike(anonymization_type_name),
                *filters,
            )
        )
        .first()
    ):
        raise DefaultException("anonymization_type_exists", code=409)
