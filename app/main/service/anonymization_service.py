from sqlalchemy import bindparam, create_engine, func, insert, select, update
from sqlalchemy_utils import create_database, database_exists, drop_database

from app.main import db
from app.main.config import app_config
from app.main.exceptions import DefaultException
from app.main.model import AnonymizationRecord, Table, User
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
from app.main.service.encryption_service import decrypt_row
from app.main.service.global_service import (
    create_table_connection,
    get_cloud_database_url,
    get_primary_key_name,
)
from app.main.service.sse_service import generate_hash_column
from app.main.service.table_service import get_sensitive_columns, get_table

_batch_selection_size = app_config.BATCH_SELECTION_SIZE


def _calculate_remove_anonymization_progress(
    table: Table, number_row_selected: int, number_row_total: int
) -> None:
    table.anonymization_progress = 100 - int(
        (number_row_selected / number_row_total) * 100
    )
    db.session.commit()


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

            table.anonymization_progress = int(
                ((index + 1) / len(anonymization_records)) * 50
            )
            db.session.commit()

        table.anonymization_progress = 50
        db.session.commit()

        cloud_table_connection = generate_hash_column(
            client_database_id=client_database.id,
            client_database_url=client_database.url,
            table=table,
        )

        client_table_connection.session.commit()
        cloud_table_connection.session.commit()
        table.anonymization_progress = 100

    except:
        client_table_connection.session.rollback()
        cloud_table_connection.session.rollback()
        table.anonymization_progress = 0
        raise DefaultException("table_not_anonymized", code=500)

    finally:
        client_table_connection.close()
        cloud_table_connection.close()
        db.session.commit()


def remove_table_anonymizaiton(
    database_id: int, table_id: int, current_user: User
) -> None:
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

    try:
        # Create client table connection
        client_table_connection = create_table_connection(
            database_url=client_database.url, table_name=table.name
        )

        # Get primary key name
        primary_key_name = get_primary_key_name(
            database_id=database_id, table_name=table.name
        )

        # Proxy to get data on batch
        results_proxy = client_table_connection.session.execute(
            select(
                client_table_connection.get_column(column_name=primary_key_name)
            ).select_from(client_table_connection.table)
        )

        # Getting rows database
        results = results_proxy.fetchmany(_batch_selection_size)

        # Start number row selected
        number_row_selected = 0

        # Get number row total
        number_row_total = (
            client_table_connection.session.query(func.count())
            .select_from(client_table_connection.table)
            .scalar()
        )

        # Encrypt database table
        while results:
            decrypted_rows = []
            primary_key_values = [row[0] for row in results]

            for primary_key_value in primary_key_values:
                decrypted_row = decrypt_row(
                    database_id=database_id,
                    table_id=table_id,
                    data={
                        "search_type": "primary_key",
                        "search_value": str(primary_key_value),
                    },
                    current_user=current_user,
                )

                decrypted_row[f"b_{primary_key_name}"] = primary_key_value
                decrypted_row.pop(primary_key_name, None)
                decrypted_rows.append(decrypted_row)

            client_table_connection.session.execute(
                update(client_table_connection.table).where(
                    client_table_connection.get_column(column_name=primary_key_name)
                    == bindparam(f"b_{primary_key_name}")
                ),
                decrypted_rows,
            )

            number_row_selected += _batch_selection_size

            _calculate_remove_anonymization_progress(
                table=table,
                number_row_selected=number_row_selected,
                number_row_total=number_row_total,
            )

            # Getting next rows database
            results = results_proxy.fetchmany(_batch_selection_size)

        table.encryption_progress = 0
        table.anonymization_progress = 0
        client_table_connection.session.commit()

    except:
        table.encryption_progress = 100
        table.anonymization_progress = 100
        client_table_connection.session.rollback()
        db.session.rollback()
        raise DefaultException("anonymization_not_removed", code=500)

    finally:
        db.session.commit()
        client_table_connection.close()


def get_anonymization_progress(
    database_id: int, table_id: int, current_user: User
) -> None:
    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    return {"progress": table.anonymization_progress}


def get_remove_anonymization_progress(
    database_id: int, table_id: int, current_user: User
) -> None:
    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    return {"progress": table.remove_anonymization_progress}
