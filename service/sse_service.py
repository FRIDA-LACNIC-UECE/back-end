from sqlalchemy import (
    create_engine, MetaData, Table, 
    select, update
)
from sqlalchemy.orm import sessionmaker, Session

from service import rsa_service


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
    table_client_db = Table(src_table, engine_cloud_db._metadata)
    
    # Run paginate
    query = session_cloud_db.query(
        table_client_db
    ).filter(
        table_client_db.c[0] >= (page*per_page), 
        table_client_db.c[0] <= ((page+1)*per_page)
    )

    results = {}
    results['primary_key'] = []
    results['row_hash'] = []
    for row in query:
        results['primary_key'].append(row[0])
        results['row_hash'].append(row[1])

    return results
    

def line_by_hash(src_db_cloud_path, src_table, hash):

    # Creating connection with client database
    srcEngineCloud = create_engine(src_db_cloud_path)
    connectionCloud= srcEngineCloud.connect()

    # Adding "_index" into table name of cloud database
    cloud_table = src_table

    # Getting columns names of client database
    result = connectionCloud.execute(f"SELECT * FROM {cloud_table}")
    columns_list = list(result.keys())
    print(columns_list)

    # create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    SourceSessionCloud = sessionmaker(srcEngineCloud)
    srcEngineCloud._metadata = MetaData(bind=srcEngineCloud)
    srcEngineCloud._metadata.reflect(srcEngineCloud)  # get columns from existing table
    srcEngineCloud._metadata.tables[cloud_table].columns = [
        i for i in srcEngineCloud._metadata.tables[f"{cloud_table}"].columns if (i.name in columns_list)]
    srcTableCloud= Table(cloud_table, srcEngineCloud._metadata)
    sourceSessionCloud = SourceSessionCloud()

    statement = sourceSessionCloud.query(srcTableCloud).filter(srcTableCloud.c[-1] == hash)

    result = sourceSessionCloud.execute(statement)

    result = list(map(list, result))[0]

    id_current = result[0]

    del result[0]
    del result[-1]

    privateKey, publicKey = rsa_service.loadKeys()

    result = rsa_service.dencrypt_list(data_list=result, key=privateKey)

    result.insert(0, id_current)

    return result


def line_by_id(src_db_cloud_path, src_table, id):

    # Creating connection with client database
    srcEngineCloud = create_engine(src_db_cloud_path)
    connectionCloud= srcEngineCloud.connect()

    # Adding "_index" into table name of cloud database
    cloud_table = src_table + "_index"

    # Getting columns names of client database
    result = connectionCloud.execute(f"SELECT * FROM {cloud_table}")
    columns_list = list(result.keys())
    print(columns_list)

    # create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    SourceSessionCloud = sessionmaker(srcEngineCloud)
    srcEngineCloud._metadata = MetaData(bind=srcEngineCloud)
    srcEngineCloud._metadata.reflect(srcEngineCloud)  # get columns from existing table
    srcEngineCloud._metadata.tables[cloud_table].columns = [
        i for i in srcEngineCloud._metadata.tables[f"{cloud_table}"].columns if (i.name in columns_list)]
    srcTableCloud= Table(cloud_table, srcEngineCloud._metadata)
    sourceSessionCloud = SourceSessionCloud()

    statement = sourceSessionCloud.query(srcTableCloud).filter(srcTableCloud.c[0] == id)

    result = sourceSessionCloud.execute(statement)

    return list(map(list, result))[0]