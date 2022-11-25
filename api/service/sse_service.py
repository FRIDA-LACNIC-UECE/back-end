import hashlib

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, func, inspect, select, update
from sqlalchemy.orm import Session, sessionmaker


def update_hash_column(
    engine_client_db,
    engine_cloud_db,
    table_client_db,
    table_cloud_db,
    src_table,
    raw_data,
):

    session_cloud_db = Session(engine_cloud_db)  # Section to run sql operation

    for row in range(raw_data.shape[0]):
        record = raw_data.iloc[row].values
        record = list(record)
        new_record = str(record)
        hashed_line = hashlib.sha256(new_record.encode("utf-8")).hexdigest()

        stmt = (
            update(table_cloud_db)
            .where(table_cloud_db.c[0] == raw_data.iloc[row]["id"])
            .values(line_hash=hashed_line)
        )

        session_cloud_db.execute(stmt)

    session_cloud_db.commit()
    session_cloud_db.close()


def generate_hash_column(src_client_db_path, src_cloud_db_path, src_table):

    # Creating connection with client database
    engine_client_db = create_engine(src_client_db_path)
    session_client_db = Session(engine_client_db)

    # Get columns of table
    client_columns_list = []
    insp = inspect(engine_client_db)
    columns_table = insp.get_columns(src_table)

    for c in columns_table:
        client_columns_list.append(str(c["name"]))
    # print(client_columns_list)

    # Create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    engine_client_db._metadata = MetaData(bind=engine_client_db)
    engine_client_db._metadata.reflect(
        engine_client_db
    )  # get columns from existing table
    engine_client_db._metadata.tables[src_table].columns = [
        i
        for i in engine_client_db._metadata.tables[src_table].columns
        if (i.name in client_columns_list)
    ]
    table_client_db = Table(src_table, engine_client_db._metadata)

    # Creating connection with user database
    engine_cloud_db = create_engine(src_cloud_db_path)
    session_user_db = Session(engine_cloud_db)

    # Get columns of table
    client_cloud_list = []
    insp = inspect(engine_cloud_db)
    columns_table = insp.get_columns(src_table)

    for c in columns_table:
        client_cloud_list.append(str(c["name"]))

    # Create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    engine_cloud_db._metadata = MetaData(bind=engine_cloud_db)
    engine_cloud_db._metadata.reflect(
        engine_cloud_db
    )  # get columns from existing table
    engine_cloud_db._metadata.tables[src_table].columns = [
        i
        for i in engine_cloud_db._metadata.tables[src_table].columns
        if (i.name in client_cloud_list)
    ]
    table_cloud_db = Table(src_table, engine_cloud_db._metadata)

    # Commit changes on Cloud Database
    session_user_db.commit()
    session_user_db.close()

    # Generate hashs
    size = 1000
    statement = select(table_client_db)
    results_proxy = session_client_db.execute(statement)  # Proxy to get data on batch
    results = results_proxy.fetchmany(size)  # Getting data

    while results:
        from_db = []

        for result in results:
            from_db.append(list(result))

        session_client_db.close()

        raw_data = pd.DataFrame(from_db, columns=client_columns_list)

        results = results_proxy.fetchmany(size)  # Getting data

        update_hash_column(
            engine_client_db,
            engine_cloud_db,
            table_client_db,
            table_cloud_db,
            src_table,
            raw_data,
        )


def include_hash_rows(src_db_cloud_path, src_table, hash_rows):

    # Creating connection with client database
    srcEngineCloud = create_engine(src_db_cloud_path)
    connectionCloud = srcEngineCloud.connect()

    # Adding "_index" into table name of cloud database
    cloud_table = src_table

    # Getting columns names of client database
    result = connectionCloud.execute(f"SELECT * FROM {cloud_table}")
    columns_list = list(result.keys())

    # create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    SourceSessionCloud = sessionmaker(srcEngineCloud)
    srcEngineCloud._metadata = MetaData(bind=srcEngineCloud)
    srcEngineCloud._metadata.reflect(srcEngineCloud)  # get columns from existing table
    srcEngineCloud._metadata.tables[cloud_table].columns = [
        i
        for i in srcEngineCloud._metadata.tables[f"{cloud_table}"].columns
        if (i.name in columns_list)
    ]
    srcTableCloud = Table(cloud_table, srcEngineCloud._metadata)
    sourceSessionCloud = SourceSessionCloud()

    for row in hash_rows:
        statement = (
            update(srcTableCloud)
            .where(srcTableCloud.c[0] == row["id"])
            .values(line_hash=row["line_hash"])
        )

        sourceSessionCloud.execute(statement)

    sourceSessionCloud.commit()

    return


def include_column_hash(src_db_cloud_path, src_db_user_path, src_table):

    # Creating connection with client database
    srcEngineCloud = create_engine(src_db_cloud_path)
    connectionCloud = srcEngineCloud.connect()

    # Adding "_index" into table name of cloud database
    cloud_table = src_table

    # Getting columns names of client database
    result = connectionCloud.execute(f"SELECT * FROM {cloud_table}")
    columns_list = list(result.keys())

    # create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    SourceSessionCloud = sessionmaker(srcEngineCloud)
    srcEngineCloud._metadata = MetaData(bind=srcEngineCloud)
    srcEngineCloud._metadata.reflect(srcEngineCloud)  # get columns from existing table
    srcEngineCloud._metadata.tables[cloud_table].columns = [
        i
        for i in srcEngineCloud._metadata.tables[f"{cloud_table}"].columns
        if (i.name in columns_list)
    ]
    srcTableCloud = Table(cloud_table, srcEngineCloud._metadata)
    sourceSessionCloud = SourceSessionCloud()

    # create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    srcEngineUser = create_engine(src_db_user_path)
    SourceSessionUser = sessionmaker(srcEngineUser)
    srcEngineUser._metadata = MetaData(bind=srcEngineUser)
    srcEngineUser._metadata.reflect(srcEngineUser)  # get columns from existing table
    srcEngineUser._metadata.tables[src_table].columns = [
        i
        for i in srcEngineUser._metadata.tables[src_table].columns
        if (i.name in ["id", "line_hash"])
    ]
    srcTableUser = Table(src_table, srcEngineUser._metadata)
    sourceSessionUser = SourceSessionUser()

    first_line_cloud = sourceSessionCloud.query(srcTableCloud).first()
    print(first_line_cloud)

    first_line_user = sourceSessionUser.query(srcTableUser).first()
    print(first_line_user)

    # First id available
    id = first_line_cloud.id

    size = 1000
    statement = select(srcTableUser)
    results_proxy = sourceSessionUser.execute(statement)  # Proxy to get data on batch
    results = results_proxy.fetchmany(size)  # Getting data

    while results:
        for result in results:
            # print(result.line_hash)
            statement = (
                update(srcTableCloud)
                .where(srcTableCloud.c[0] == id)
                .values(line_hash=result.line_hash)
            )

            id += 1

            sourceSessionCloud.execute(statement)

        sourceSessionCloud.commit()

        results = results_proxy.fetchmany(size)  # Getting data

    return


def show_hash_rows(src_cloud_db_path, src_table, page, per_page):

    # Creating connection with original database
    engine_cloud_db = create_engine(src_cloud_db_path)
    session_cloud_db = Session(engine_cloud_db)

    # Create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    engine_cloud_db._metadata = MetaData(bind=engine_cloud_db)
    engine_cloud_db._metadata.reflect(engine_cloud_db)

    engine_cloud_db._metadata.tables[src_table].columns = [
        i
        for i in engine_cloud_db._metadata.tables[src_table].columns
        if (i.name in ["id", "line_hash"])
    ]
    table_cloud_db = Table(src_table, engine_cloud_db._metadata)

    primary_key_value_min_limit = session_cloud_db.query(
        func.min(table_cloud_db.c[0])
    ).scalar()
    primary_key_value_max_limit = session_cloud_db.query(
        func.max(table_cloud_db.c[0])
    ).scalar()

    # Run paginate
    query = session_cloud_db.query(table_cloud_db).filter(
        table_cloud_db.c[0] >= primary_key_value_min_limit + (page * per_page),
        table_cloud_db.c[0] <= primary_key_value_min_limit + ((page + 1) * per_page),
    )

    results = {}
    results["primary_key"] = []
    results["row_hash"] = []
    for row in query:
        results["primary_key"].append(row[0])
        results["row_hash"].append(row[1])

    session_cloud_db.commit()
    session_cloud_db.close()

    return results, primary_key_value_min_limit, primary_key_value_max_limit


def insert_hash_rows(src_cloud_db_path, src_table, primary_key_list):

    # Creating connection with original database
    engine_cloud_db = create_engine(src_cloud_db_path)
    session_cloud_db = Session(engine_cloud_db)

    # Create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    engine_cloud_db._metadata = MetaData(bind=engine_cloud_db)
    engine_cloud_db._metadata.reflect(engine_cloud_db)

    engine_cloud_db._metadata.tables[src_table].columns = [
        i
        for i in engine_cloud_db._metadata.tables[src_table].columns
        if (i.name in ["id", "line_hash"])
    ]
    table_cloud_db = Table(src_table, engine_cloud_db._metadata)

    # session_cloud_db.commit()
    # session_cloud_db.close()

    return 200


def delete_hash_rows(src_cloud_db_path, src_table, primary_key_list):

    # Creating connection with original database
    engine_cloud_db = create_engine(src_cloud_db_path)
    session_cloud_db = Session(engine_cloud_db)

    # Create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    engine_cloud_db._metadata = MetaData(bind=engine_cloud_db)
    engine_cloud_db._metadata.reflect(engine_cloud_db)

    engine_cloud_db._metadata.tables[src_table].columns = [
        i
        for i in engine_cloud_db._metadata.tables[src_table].columns
        if (i.name in ["id", "line_hash"])
    ]
    table_cloud_db = Table(src_table, engine_cloud_db._metadata)

    for primary_key in primary_key_list:
        session_cloud_db.query(table_cloud_db).filter(
            table_cloud_db.c[0] == primary_key
        ).delete()

    session_cloud_db.commit()
    session_cloud_db.close()

    return 200
