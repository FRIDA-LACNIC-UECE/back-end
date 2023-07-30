from sqlalchemy import func, select, update
from werkzeug.datastructures import ImmutableMultiDict

from app.main.exceptions import DefaultException
from app.main.model import User


def show_rows_hash(
    database_id: int, table_id: int, params: ImmutableMultiDict, current_user: User
) -> dict[str, any]:
    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )
    page = params.get("page", type=int)
    per_page = params.get("per_page", type=int)

    # Get cloud database url
    cloud_database_url = get_cloud_database_url(database_id=database_id)

    # Get primary key name
    primary_key_name = get_primary_key_name(
        database_id=database_id, table_name=table.name
    )

    # Create cloud table connection
    cloud_table_connection = create_table_connection(
        database_url=cloud_database_url,
        table_name=table.name,
        columns_list=[primary_key_name, "line_hash"],
    )

    # Run paginate
    query = cloud_table_connection.session.query(cloud_table_connection.table).filter(
        cloud_table_connection.get_column(column_name=primary_key_name)
        >= (page * per_page),
        cloud_table_connection.get_column(column_name=primary_key_name)
        <= ((page + 1) * per_page),
    )

    row_hash_list = {}
    row_hash_list["primary_key"] = []
    row_hash_list["row_hash"] = []

    for row in query:
        row_hash_list["primary_key"].append(row[0])
        row_hash_list["row_hash"].append(row[1])

    # Get limits primary key value
    primary_key_value_min_limit = cloud_table_connection.session.query(
        func.min(cloud_table_connection.get_column(column_name=primary_key_name))
    ).scalar()
    primary_key_value_max_limit = cloud_table_connection.session.query(
        func.max(cloud_table_connection.get_column(column_name=primary_key_name))
    ).scalar()

    cloud_table_connection.session.commit()
    cloud_table_connection.close()

    return {
        "row_hash_list": [row_hash_list],
        "primary_key_value_min_limit": primary_key_value_min_limit,
        "primary_key_value_max_limit": primary_key_value_max_limit,
    }


def process_inserts(
    database_id: int, table_id: int, data: dict[str, str], current_user: User
) -> None:
    hash_rows = data.get("hash_rows")

    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    # Get Cloud Database Url
    cloud_database_url = get_cloud_database_url(database_id=database_id)

    # Get primary key column name
    primary_key_name = get_primary_key_name(
        database_id=database_id, table_name=table.name
    )

    # Create cloud table connection
    cloud_table_connection = create_table_connection(
        database_url=cloud_database_url,
        table_name=table.name,
        columns_list=[primary_key_name, "line_hash"],
    )

    try:
        for row in hash_rows:
            statement = (
                update(cloud_table_connection.table)
                .where(
                    cloud_table_connection.get_column(column_name=primary_key_name)
                    == row["primary_key"]
                )
                .values(line_hash=row["line_hash"])
            )

            cloud_table_connection.session.execute(statement)

        cloud_table_connection.session.commit()

    except:
        if cloud_table_connection is not None:
            cloud_table_connection.session.rollback()

        raise DefaultException("inserts_not_processed", code=500)

    finally:
        if cloud_table_connection is not None:
            cloud_table_connection.close()


def process_updates(
    database_id: int, table_id: int, data: dict[str, str], current_user: User
) -> None:
    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    primary_key_list = data.get("primary_key_list")

    # Get client database
    client_database = get_database(database_id=database_id)

    # Create client table connection
    client_table_connection = create_table_connection(
        database_url=client_database.url,
        table_name=table.name,
    )

    # Get primary key column name
    primary_key_name = get_primary_key_name(
        database_id=database_id, table_name=table.name
    )

    # Get sensitive columns of Client Database
    sensitive_columns = get_sensitive_columns(
        database_id=database_id, table_id=table.id
    )["sensitive_column_names"]

    try:
        # Get original rows ​​that have been updated
        rows_list = []
        update_log = {}

        for primary_key_value in primary_key_list:
            update_log["primary_key_value"] = primary_key_value

            found_row = decrypt_row(
                database_id=database_id,
                table_id=table_id,
                data={
                    "search_type": "primary_key",
                    "search_value": primary_key_value,
                },
            )

            found_row = found_row

            anonymized_row = found_row.copy()
            anonymized_row = anonymization_database_rows(
                database_id=database_id,
                table_id=table_id,
                data={
                    "rows_to_anonymization": [anonymized_row],
                    "insert_database": False,
                },
                current_user=current_user,
            )["rows_anonymized"]
            anonymized_row = anonymized_row[0]

            statement = select(client_table_connection.table).where(
                client_table_connection.get_column(column_name=primary_key_name)
                == primary_key_value
            )
            client_row = client_table_connection.session.execute(statement)
            client_row = [row._asdict() for row in client_row][0]

            update_log["columns"] = []
            update_log["values"] = []
            for sensitive_column in sensitive_columns:
                if type(client_row[sensitive_column]).__name__ == "date":
                    if anonymized_row[sensitive_column] != client_row[
                        sensitive_column
                    ].strftime("%Y-%m-%d"):
                        found_row[sensitive_column] = client_row[sensitive_column]
                        update_log["columns"].append(sensitive_column)
                        update_log["values"].append(
                            client_row[sensitive_column].strftime("%Y-%m-%d")
                        )

                else:
                    if anonymized_row[sensitive_column] != client_row[sensitive_column]:
                        found_row[sensitive_column] = client_row[sensitive_column]
                        update_log["columns"].append(sensitive_column)
                        update_log["values"].append(client_row[sensitive_column])

            rows_list.append(found_row)

            updates_log(database_id=database_id, table_name=table.name, data=update_log)

        client_table_connection.session.commit()

        encrypt_database_row(
            database_id=database_id,
            table_id=table_id,
            data={
                "table_name": table.name,
                "rows_to_encrypt": rows_list.copy(),
                "update_database": True,
            },
            current_user=current_user,
        )
        client_table_connection.session.commit()

        anonymized_rows = anonymization_database_rows(
            database_id=database_id,
            table_id=table_id,
            data={
                "table_name": table.name,
                "rows_to_anonymization": rows_list.copy(),
                "insert_database": True,
            },
            current_user=current_user,
        )["rows_anonymized"]
        client_table_connection.session.commit()

        cloud_table_connection = generate_hash_rows(
            database_id=database_id,
            table_id=table.id,
            result_query=anonymized_rows,
            current_user=current_user,
        )
        cloud_table_connection.session.commit()

    except:
        if client_table_connection is not None:
            client_table_connection.session.rollback()

        if cloud_table_connection is not None:
            cloud_table_connection.session.rollback()

        raise DefaultException("updates_not_processed", code=500)

    finally:
        if client_table_connection is not None:
            client_table_connection.close()

        if cloud_table_connection is not None:
            cloud_table_connection.close()


def process_deletions(
    database_id: int, table_id: int, data: dict[str, str], current_user: User
) -> None:
    table = get_table(
        database_id=database_id, table_id=table_id, current_user=current_user
    )

    primary_key_list = data.get("primary_key_list")

    # Get database information by id
    cloud_database_url = get_cloud_database_url(database_id=database_id)

    # Create cloud table connection
    cloud_table_connection = create_table_connection(
        database_url=cloud_database_url, table_name=table.name
    )

    # Get primary key column name
    primary_key_name = get_primary_key_name(
        database_id=database_id, table_name=table.name
    )

    try:
        for primary_key_value in primary_key_list:
            cloud_table_connection.session.query(cloud_table_connection.table).filter(
                cloud_table_connection.get_column(column_name=primary_key_name)
                == primary_key_value
            ).delete()

            cloud_table_connection.session.commit()

    except:
        if cloud_table_connection is not None:
            cloud_table_connection.session.rollback()

        raise DefaultException("deletes_not_processed", code=500)

    finally:
        if cloud_table_connection is not None:
            cloud_table_connection.close()


from app.main.service.anonymization_service import anonymization_database_rows
from app.main.service.database_service import get_database
from app.main.service.encryption_service import decrypt_row, encrypt_database_row
from app.main.service.global_service import (
    create_table_connection,
    get_cloud_database_url,
    get_primary_key_name,
)
from app.main.service.sql_log_service import updates_log
from app.main.service.sse_service import generate_hash_rows
from app.main.service.table_service import get_sensitive_columns, get_table
