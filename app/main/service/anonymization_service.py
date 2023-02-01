from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists

from app.main.exceptions import DefaultException, ValidationException
from app.main.model import AnonymizationRecord, AnonymizationType, User
from app.main.service.anonymization_types import (
    cpf_anonymizer_service,
    date_anonymizer_service,
    email_anonymizer_service,
    ip_anonymizer_service,
    named_entities_anonymizer_service,
    rg_anonymizer_service,
)
from app.main.service.database_service import get_database, get_database_url
from app.main.service.sse_service import generate_hash_column


def anonymization_table(database_url: str, database_id: int, table_name: str) -> int:

    # Get chosen columns
    anonymization_records = AnonymizationRecord.query.filter_by(
        database_id=database_id, table=table_name
    ).all()

    if not anonymization_records:
        raise ValidationException(
            errors={"anonymization_record": "anonymization_record_invalid_data"},
            message="Input payload validation failed",
        )

    # Run anonymization for each anonymization types
    for anonymization_record in anonymization_records:

        # Get anonymization type name by id
        anonymization_type_name = (
            AnonymizationType.query.filter_by(
                id=anonymization_record.anonymization_type_id
            )
            .first()
            .name
        )

        if not anonymization_record.columns:
            continue

        # Run anonymization
        if anonymization_type_name == "named_entities_anonymizer":
            named_entities_anonymizer_service.anonymization_database(
                src_client_db_path=database_url,
                table_name=anonymization_record.table,
                columns_to_anonymize=anonymization_record.columns,
            )
            # print("\n\n name_entities_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "date_anonymizer":
            date_anonymizer_service.anonymization_database(
                src_client_db_path=database_url,
                table_name=anonymization_record.table,
                columns_to_anonymize=anonymization_record.columns,
            )
            # print("\n\n date_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "email_anonymizer":
            email_anonymizer_service.anonymization_database(
                src_client_db_path=database_url,
                table_name=anonymization_record.table,
                columns_to_anonymize=anonymization_record.columns,
            )
            # print("\n\n email_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "ip_anonymizer":
            ip_anonymizer_service.anonymization_database(
                src_client_db_path=database_url,
                table_name=anonymization_record.table,
                columns_to_anonymize=anonymization_record.columns,
            )
            # print("\n\n ip_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "rg_anonymizer":
            rg_anonymizer_service.anonymization_database(
                src_client_db_path=database_url,
                table_name=anonymization_record.table,
                columns_to_anonymize=anonymization_record.columns,
            )
            # print("\n\n rg_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "cpf_anonymizer":
            cpf_anonymizer_service.anonymization_database(
                src_client_db_path=database_url,
                table_name=anonymization_record.table,
                columns_to_anonymize=anonymization_record.columns,
            )
            # print("\n\n cpf_anonymizer anonimizando\n\n")

        else:
            return 400

    return 200


def anonymization_database(current_user: User, data: dict[str, str]) -> None:

    database_id = data.get("database_id")
    table_name = data.get("table_name")

    client_database = get_database(database_id=database_id)

    if client_database.user_id != current_user.id:
        raise DefaultException("unauthorized_user", code=401)

    client_database_url = get_database_url(database_id=database_id)

    # Check connection with database found
    engine_client_db = create_engine(client_database_url)
    if not database_exists(engine_client_db.url):
        raise DefaultException("database_not_conected", code=409)

    anonymization_table(
        database_url=client_database_url,
        database_id=database_id,
        table_name=table_name,
    )

    generate_hash_column(
        user_id=current_user.id,
        database_id=database_id,
        table_name=table_name,
    )
