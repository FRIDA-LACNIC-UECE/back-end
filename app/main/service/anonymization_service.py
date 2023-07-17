from app.main import db
from app.main.exceptions import DefaultException
from app.main.model import AnonymizationRecord, User
from app.main.service.anonymization_type_service import get_anonymization_type
from app.main.service.anonymization_types import (
    cpf_anonymizer_service,
    date_anonymizer_service,
    email_anonymizer_service,
    ip_anonymizer_service,
    named_entities_anonymizer_service,
    rg_anonymizer_service,
)
from app.main.service.database_service import get_database, get_database_tables_names
from app.main.service.global_service import create_table_connection
from app.main.service.sse_service import generate_hash_column
from app.main.service.table_service import get_table


def anonymization_database_rows(
    database_id: int, table_id: int, data: dict[str, str], current_user: User
) -> dict:
    rows_to_anonymization = data.get("rows_to_anonymization")
    insert_database = data.get("insert_database")

    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    client_database = get_database(
        database_id=database_id, current_user=current_user, verify_connection=True
    )

    database_tables_names = get_database_tables_names(
        database_id=database_id, current_user=current_user
    )

    if not table.name in database_tables_names["table_names"]:
        raise DefaultException("outdated_table", code=409)

    # Create client table connection
    client_table_connection = create_table_connection(
        database_url=client_database.url, table_name=table.name
    )

    # Get sensitive columns
    anonymization_records = AnonymizationRecord.query.filter_by(table_id=table.id).all()

    if not anonymization_records:
        raise DefaultException("anonymization_record_not_found", code=404)

    # Run anonymization for each anonymization types
    try:
        for anonymization_record in anonymization_records:
            if not anonymization_record.columns:
                continue

            anonymization_type = get_anonymization_type(
                anonymization_type_id=anonymization_record.anonymization_type_id
            )

            eval(
                f"{anonymization_type.name}_service.anonymization_database_rows(\
                    client_table_connection=client_table_connection,\
                    columns_to_anonymize=anonymization_record.columns,\
                    rows_to_anonymize=rows_to_anonymization,\
                    insert_database=insert_database,\
                    )"
            )
    except:
        client_table_connection.session.rollback()
        raise DefaultException("database_rows_not_anonymized", code=500)

    finally:
        client_table_connection.close()

    return {"rows_anonymized": rows_to_anonymization}


def anonymization_table(database_id: int, table_id: int, current_user: User) -> int:
    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    client_database = get_database(
        database_id=database_id, current_user=current_user, verify_connection=True
    )

    database_tables_names = get_database_tables_names(
        database_id=database_id, current_user=current_user
    )

    if not table.name in database_tables_names["table_names"]:
        raise DefaultException("outdated_table", code=409)

    client_table_connection = create_table_connection(
        database_url=client_database.url, table_name=table.name
    )

    anonymization_records = AnonymizationRecord.query.filter_by(table_id=table.id).all()

    if not anonymization_records:
        raise DefaultException("anonymization_record_not_found", code=404)

    try:
        for index, anonymization_record in enumerate(anonymization_records):
            if not anonymization_record.columns:
                continue

            anonymization_type = get_anonymization_type(
                anonymization_type_id=anonymization_record.anonymization_type_id
            )

            eval(
                f"{anonymization_type.name}_service.anonymization_database(\
                database_id=client_database.id,\
                client_table_connection=client_table_connection,\
                columns_to_anonymize=anonymization_record.columns,\
                )"
            )

            table.anonimyzation_progress = int(
                ((index + 1) / len(anonymization_records)) * 50
            )
            db.session.commit()

        table.anonimyzation_progress = 50
        db.session.commit()

        cloud_table_connection = generate_hash_column(
            client_database_id=client_database.id,
            client_database_url=client_database.url,
            table=table,
        )

        client_table_connection.session.commit()
        cloud_table_connection.session.commit()
        table.anonimyzation_progress = 100

    except:
        client_table_connection.session.rollback()
        cloud_table_connection.session.rollback()
        table.anonimyzation_progress = 0
        raise DefaultException("table_not_anonymized", code=500)

    finally:
        client_table_connection.close()
        cloud_table_connection.close()
        db.session.commit()


def get_anonymization_progress(
    database_id: int, table_id: int, current_user: User
) -> None:
    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    return {"progress": table.anonimyzation_progress}
