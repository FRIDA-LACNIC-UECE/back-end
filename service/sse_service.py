from sqlalchemy import (
    create_engine, MetaData, Table, 
    select, update, func
)
from sqlalchemy.orm import sessionmaker, Session

from service.rsa_service import decrypt_dict, loadKeys
from service.database_service import get_columns_database, create_table_session, get_primary_key


def include_hash_rows(src_db_cloud_path, src_table, hash_rows):
    
    # Creating connection with client database
    srcEngineCloud = create_engine(src_db_cloud_path)
    connectionCloud= srcEngineCloud.connect()

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
        i for i in srcEngineCloud._metadata.tables[f"{cloud_table}"].columns if (i.name in columns_list)]
    srcTableCloud= Table(cloud_table, srcEngineCloud._metadata)
    sourceSessionCloud = SourceSessionCloud()
   
    for row in hash_rows:
        statement = (
            update(srcTableCloud).
            where(srcTableCloud.c[0] == row['id']).
            values(line_hash=row["line_hash"])
        )

        sourceSessionCloud.execute(statement)
    
    sourceSessionCloud.commit()

    return


def include_column_hash(src_db_cloud_path, src_db_user_path, src_table):
    
    # Creating connection with client database
    srcEngineCloud = create_engine(src_db_cloud_path)
    connectionCloud= srcEngineCloud.connect()

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
        i for i in srcEngineCloud._metadata.tables[f"{cloud_table}"].columns if (i.name in columns_list)]
    srcTableCloud= Table(cloud_table, srcEngineCloud._metadata)
    sourceSessionCloud = SourceSessionCloud()
    
    # create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    srcEngineUser = create_engine(src_db_user_path)
    SourceSessionUser = sessionmaker(srcEngineUser)
    srcEngineUser._metadata = MetaData(bind=srcEngineUser)
    srcEngineUser._metadata.reflect(srcEngineUser)  # get columns from existing table
    srcEngineUser._metadata.tables[src_table].columns = [
        i for i in srcEngineUser._metadata.tables[src_table].columns if (i.name in ['id', 'line_hash'])]
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
    results_proxy = sourceSessionUser.execute(statement) # Proxy to get data on batch
    results = results_proxy.fetchmany(size) # Getting data

    while results:
        for result in results:
            #print(result.line_hash)
            statement = (
                update(srcTableCloud).
                where(srcTableCloud.c[0] == id).
                values(line_hash=result.line_hash)
            )

            id += 1
            
            sourceSessionCloud.execute(statement)
        
        sourceSessionCloud.commit()

        results = results_proxy.fetchmany(size) # Getting data

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
        i for i in engine_cloud_db._metadata.tables[src_table].columns if (i.name in ['id', 'line_hash'])]
    table_cloud_db = Table(src_table, engine_cloud_db._metadata)

    primary_key_value_min_limit = session_cloud_db.query(func.min(table_cloud_db.c[0])).scalar()
    primary_key_value_max_limit = session_cloud_db.query(func.max(table_cloud_db.c[0])).scalar()
    
    # Run paginate
    query = session_cloud_db.query(
        table_cloud_db
    ).filter(
        table_cloud_db.c[0] >= primary_key_value_min_limit + (page * per_page), 
        table_cloud_db.c[0] <= primary_key_value_min_limit + ((page + 1) *per_page)
    )

    results = {}
    results['primary_key'] = []
    results['row_hash'] = []
    for row in query:
        results['primary_key'].append(row[0])
        results['row_hash'].append(row[1])

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
        i for i in engine_cloud_db._metadata.tables[src_table].columns if (i.name in ['id', 'line_hash'])]
    table_cloud_db = Table(src_table, engine_cloud_db._metadata)
    
    #session_cloud_db.commit()
    #session_cloud_db.close()

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
        i for i in engine_cloud_db._metadata.tables[src_table].columns if (i.name in ['id', 'line_hash'])]
    table_cloud_db = Table(src_table, engine_cloud_db._metadata)

    for primary_key in primary_key_list:
        session_cloud_db.query(
            table_cloud_db
        ).filter(
            table_cloud_db.c[0] == primary_key
        ).delete()
    
    session_cloud_db.commit()
    session_cloud_db.close()

    return 200
    

def row_by_id(src_db_cloud_path, table_name, row_id):

    # Creating connection with client database
    srcEngineCloud = create_engine(src_db_cloud_path)
    connectionCloud= srcEngineCloud.connect()

    # Getting columns names of client database
    result = connectionCloud.execute(f"SELECT * FROM {table_name}")
    columns_list = list(result.keys())
    print(columns_list)

    # create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    SourceSessionCloud = sessionmaker(srcEngineCloud)
    srcEngineCloud._metadata = MetaData(bind=srcEngineCloud)
    srcEngineCloud._metadata.reflect(srcEngineCloud)  # get columns from existing table
    srcEngineCloud._metadata.tables[table_name].columns = [
        i for i in srcEngineCloud._metadata.tables[f"{table_name}"].columns if (i.name in columns_list)]
    srcTableCloud= Table(table_name, srcEngineCloud._metadata)
    sourceSessionCloud = SourceSessionCloud()

    statement = sourceSessionCloud.query(srcTableCloud).filter(srcTableCloud.c[0] == row_id)

    result = sourceSessionCloud.execute(statement)

    return list(map(list, result))[0]


def row_by_hash(src_cloud_db_path, table_name, row_hash, public_key_str, private_key_str):

    # Creating connection with client database
    engine_cloud_db = create_engine(src_cloud_db_path)

    # Create table object of Cloud Database and 
    # session of Cloud Database to run sql operations
    table_cloud_db, session_cloud_db = create_table_session(
        src_db_path=src_cloud_db_path, 
        table_name=table_name
    )

    # Get data by row_hash
    result_query = session_cloud_db.query(table_cloud_db).filter(table_cloud_db.c[-1]==row_hash)
    dict_query_data = [row._asdict() for row in result_query][0]
    print(dict_query_data)

    # Load rsa keys
    public_key, private_key = loadKeys(public_key_str, private_key_str)
    print(private_key)
    
    # Get name primary key
    #primary_key_name = get_primary_key(src_cloud_db_path, table_name)
    primary_key_name = "id"

    # Remove primary key
    primary_key_value = dict_query_data[f"{primary_key_name}"]
    remove_primary_key_response = dict_query_data.pop(primary_key_name, None)

    if not remove_primary_key_response:
        return 400
    
    # Decrypted dict_query_data
    dict_decrypted = decrypt_dict(dict_query_data, private_key)

    return dict_decrypted