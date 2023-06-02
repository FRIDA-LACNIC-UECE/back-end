from faker import Faker
from sqlalchemy import select

from app.main.config import app_config
from app.main.service.global_service import TableConnection, get_primary_key_name

_batch_selection_size = app_config.BATCH_SELECTION_SIZE

DATABASE_ID = None
TABLE_NAME = None


def anonymization_rg(seed: int) -> str:
    """
    This function generates new rg.

    Parameters
    ----------
    seed : int
        Seed value.

    Returns
    -------
    str
        New rg generated.
    """

    # Define generator seed
    Faker.seed(seed)

    # Create rg generator and define data language
    faker = Faker(["pt_BR"])

    # Generate new rg
    new_rg = faker.rg()

    return str(new_rg)


def anonymization_data(
    primary_key_name: str,
    rows_to_anonymize: list[dict],
    columns_to_anonymize: list[str],
) -> list[dict]:
    """
    This function anonymizes each column of each given row with
    the RG Anonymizar.

    Parameters
    ----------
    primary_key_name : str
        Primary key column name.

    row_to_anonymize : list[dict]
        Rows provided for anonymization.

    columns_to_anonymize : list[str]
        Column names chosen for anonymization.

    Returns
    -------
    list[dict]
        Anonymized rows
    """

    for number_row in range(len(rows_to_anonymize)):
        # Get primary key value
        primary_key_value = rows_to_anonymize[number_row][primary_key_name]

        # Generate new rg
        new_rg = anonymization_rg(seed=primary_key_value)

        # Add new rg in chosen columns to anonymize
        for column in columns_to_anonymize:
            rows_to_anonymize[number_row][column] = new_rg

    return rows_to_anonymize


def anonymization_database_rows(
    client_table_connection: TableConnection,
    columns_to_anonymize: list[str],
    rows_to_anonymize: list[dict],
    insert_database: bool = True,
) -> list[dict]:
    """
    This function anonymizes each the given rows with the
    RG Anonymizer.

    Parameters
    ----------
    database_url : str
        Connection URI of Client Database.

    table_name : str
        Table name where anonymization will be performed.

    columns_to_anonymize : list[str]
        Column names chosen for anonymization.

    rows_to_anonymize : list[dict]
        Rows provided for anonymization.

    insert_database : bool
        Flag to indicate if anonymized rows will be inserted or returned.

    Returns
    -------
    list[dict]
        Anonymized rows
    """

    # Get primary key column name of Client Database
    primary_key_name = get_primary_key_name(
        database_url=client_table_connection.engine.url,
        table_name=client_table_connection.table_name,
    )

    # Run anonymization
    anonymized_rows = anonymization_data(
        primary_key_name=primary_key_name,
        rows_to_anonymize=rows_to_anonymize,
        columns_to_anonymize=columns_to_anonymize,
    )

    # Insert anonymized rows in database
    if insert_database:
        for anonymized_row in anonymized_rows:
            client_table_connection.session.query(client_table_connection.table).filter(
                client_table_connection.get_column(column_name=primary_key_name)
                == anonymized_row[f"{primary_key_name}"]
            ).update({key: anonymized_row[key] for key in columns_to_anonymize})

        client_table_connection.session.flush()

    return anonymized_rows


def anonymization_database(
    database_id,
    client_table_connection: TableConnection,
    columns_to_anonymize: list[str],
) -> None:
    """
    This function anonymizes a table with the
    RG Anonymizer.

    Parameters
    ----------
    database_url : str
        Connection URI of Client Database

    table_name : str
        Table name where anonymization will be performed

    columns_to_anonymize : list[str]
        Column names chosen for anonymization.

    Returns
    -------
    None
    """

    DATABASE_ID = database_id
    TABLE_NAME = client_table_connection.table_name

    # Get primary key column name of Client Database
    primary_key_name = get_primary_key_name(
        database_url=client_table_connection.engine.url,
        table_name=client_table_connection.table_name,
    )

    # Get rows on batch for anonymization
    results_proxy = client_table_connection.session.execute(
        select(client_table_connection.table)
    )

    results = results_proxy.fetchmany(_batch_selection_size)  # Getting rows database

    while results:
        # Transform rows from tuples to dictionary
        rows_to_anonymize = [row._asdict() for row in results]

        # Run anonymization
        anonymized_rows = anonymization_data(
            primary_key_name=primary_key_name,
            rows_to_anonymize=rows_to_anonymize,
            columns_to_anonymize=columns_to_anonymize,
        )

        # Insert anonymized rows in database
        for anonymized_row in anonymized_rows:
            client_table_connection.session.query(client_table_connection.table).filter(
                client_table_connection.get_column(column_name=primary_key_name)
                == anonymized_row[f"{primary_key_name}"]
            ).update({key: anonymized_row[key] for key in columns_to_anonymize})

        client_table_connection.session.flush()

        # Getting rows for anonymization
        results = results_proxy.fetchmany(_batch_selection_size)
