from faker import Faker
from service.database_service import (
    create_table_session,
    get_index_column_table_object,
    get_primary_key,
)
from sqlalchemy import select


def anonymization_name(seed: int) -> str:
    # Define generator seed
    Faker.seed(seed)

    # Create name generator and define data language
    faker = Faker(["pt_BR"])

    # Generate new name
    new_name = faker.name()

    return str(new_name)


def anonymization_data(
    primary_key_name: str, row_to_anonymize: dict, columns_to_anonymization: list
) -> dict:
    # Anonymize chosen columns
    for column in columns_to_anonymization:
        row_to_anonymize[column] = anonymization_name(
            seed=row_to_anonymize[primary_key_name]
        )

    return row_to_anonymize


def anonymization_one_database_row(
    src_client_db_path: str,
    table_name: str,
    columns_to_anonymization: list,
    row_to_anonymization: list,
    insert_database: bool = False,
) -> dict:

    # Get primary key of Client Database
    primary_key = get_primary_key(src_client_db_path, table_name)
    index_primary_key = get_index_column_table_object(
        src_db_path=src_client_db_path, table_name=table_name, column_name=primary_key
    )

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, table_name=table_name
    )

    # Anonymize one database row
    row_to_anonymization = anonymization_data(
        primary_key_name=primary_key,
        row_to_anonymize=row_to_anonymization,
        columns_to_anonymization=columns_to_anonymization,
    )

    # Insert row anonymized on database
    if insert_database:
        session_client_db.query(table_client_db).filter(
            table_client_db.c[index_primary_key]
            == row_to_anonymization[f"{primary_key}"]
        ).update(row_to_anonymization)

        session_client_db.commit()

    session_client_db.close()

    return row_to_anonymization


def anonymization_database_rows(
    src_client_db_path: str,
    table_name: str,
    columns_to_anonymization: list,
    rows_to_anonymization: list,
):

    # Get primary key of Client Database
    primary_key = get_primary_key(src_client_db_path, table_name)
    index_primary_key = get_index_column_table_object(
        src_db_path=src_client_db_path, table_name=table_name, column_name=primary_key
    )

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, table_name=table_name
    )

    # Run anonymization
    for row_number in range(0, len(rows_to_anonymization)):
        rows_to_anonymization[row_number] = anonymization_data(
            primary_key_name=primary_key,
            row_to_anonymize=rows_to_anonymization[row_number],
            columns_to_anonymization=columns_to_anonymization,
        )

    # Update data
    for row_anonymized in rows_to_anonymization:
        session_client_db.query(table_client_db).filter(
            table_client_db.c[index_primary_key] == row_anonymized[f"{primary_key}"]
        ).update(row_anonymized)

    session_client_db.commit()
    session_client_db.close()


def anonymization_database(src_client_db_path, table_name, columns_to_anonymization):

    # Get primary key of Client Database
    primary_key = get_primary_key(src_client_db_path, table_name)
    index_primary_key = get_index_column_table_object(
        src_db_path=src_client_db_path, table_name=table_name, column_name=primary_key
    )

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, table_name=table_name
    )

    # Get data to dataframe
    size = 1000
    statement = select(table_client_db)
    results_proxy = session_client_db.execute(statement)  # Proxy to get data on batch
    results = results_proxy.fetchmany(size)  # Getting data

    while results:
        result = [row._asdict() for row in results]

        print(result)

        for row_number in range(0, len(result)):
            result[row_number] = anonymization_data(
                primary_key_name=primary_key,
                row_to_anonymize=result[row_number],
                columns_to_anonymization=columns_to_anonymization,
            )

        # Update data
        for row_anonymized in result:
            session_client_db.query(table_client_db).filter(
                table_client_db.c[index_primary_key] == row_anonymized[f"{primary_key}"]
            ).update(row_anonymized)

        session_client_db.commit()

        results = results_proxy.fetchmany(size)  # Getting data

    session_client_db.close()

    return
