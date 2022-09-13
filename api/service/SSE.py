import numpy as np
import pandas as pd
from Crypto.Cipher import AES
from Crypto.Hash import MD5
from Crypto.Random import random
from sqlalchemy import (
    create_engine, MetaData, Table, Column, 
    inspect, Integer, select
)
from sqlalchemy.orm import Session, sessionmaker


def build_trapdoor(MK, keyword):
    keyword_index = MD5.new()
    keyword_index.update(str(keyword).encode())
    ECB_cipher = AES.new(MK.encode("utf8"), AES.MODE_ECB)
    return ECB_cipher.encrypt(keyword_index.digest())


def build_codeword(ID, trapdoor):
    ID_index = MD5.new()
    ID_index.update(str(ID).encode())
    ECB_cipher = AES.new(trapdoor, AES.MODE_ECB)
    return ECB_cipher.encrypt(ID_index.digest()).hex()


def build_index(MK, ID, record_columns_list):
    secure_index = [0] * len(record_columns_list)
    for i in range(len(record_columns_list)):
        codeword = build_codeword(ID, build_trapdoor(MK, record_columns_list[i]))
        secure_index[i] = codeword
    random.shuffle(secure_index)
    return secure_index


def searchable_encryption(srcEngineClient, src_table, srcTableClient, columns_list, master_key, srcConnectionCloud):
   
    from_db = []
    document_index = []

    session_db = Session(srcEngineClient) # Section to run sql operation

    size = 1000
    statement = select(srcTableClient)
    results_proxy = session_db.execute(statement) # Proxy to get data on batch
    results = results_proxy.fetchmany(size) # Getting data

    while results:
        #print(results[0].to_list())
        for result in results:
            from_db.append(list(result))

        results = results_proxy.fetchmany(size) # Getting data

    #print(from_db)
    session_db.close()

    raw_data = pd.DataFrame(from_db, columns=columns_list)
    id_index = raw_data['id']

    raw_data = raw_data.drop('id', axis=1)
    columns_list.remove('id')

    index_header = []
    for i in range(1, len(columns_list) + 1):
        index_header.append("index_" + str(i))

    features = list(raw_data)
    raw_data = raw_data.values

    column_number = [i for i in range(0, len(features)) if features[i] in columns_list]
    
    for row in range(raw_data.shape[0]):
        record = raw_data[row]
        record_columns_list = [record[i] for i in column_number]
        record_index = build_index(master_key, row, record_columns_list)
        document_index.append(record_index)
  
    document_index_dataframe = pd.DataFrame(np.array(document_index), columns=index_header)
    document_index_dataframe['id'] = id_index
    document_index_dataframe['line_hash'] = [None] * len(document_index_dataframe)

    print(document_index_dataframe)
    columns_list.insert(0, "id")
    document_index_dataframe = document_index_dataframe.reindex(columns=columns_list)
    
    new_file_name = src_table + "_index"

    document_index_dataframe.to_sql(new_file_name, srcConnectionCloud, if_exists='replace', index=False)
    

def encrypt_data(src_db_client_path, src_db_dest_path, src_table):

    # Creating connection with client database
    srcEngineClient = create_engine(src_db_client_path)
    connectionClient = srcEngineClient.connect()

    # Getting columns names of client database
    result = connectionClient.execute(f"SELECT * FROM {src_table}")
    columns_list = list(result.keys())

    # create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    srcEngineClient._metadata = MetaData(bind=srcEngineClient)
    srcEngineClient._metadata.reflect(srcEngineClient)  # get columns from existing table
    srcEngineClient._metadata.tables[src_table].columns = [
        i for i in srcEngineClient._metadata.tables[src_table].columns if (i.name in columns_list)]
    srcTableClient = Table(src_table, srcEngineClient._metadata)

    master_key_file_name = "./api/service/masterkey" #password autentication
    master_key = open(master_key_file_name).read()
    if len(master_key) > 16:
        print("the length of master key is larger than 16 bytes, only the first 16 bytes are used")
        master_key = bytes(master_key[:16])
    
    srcConnectionCloud = create_engine(src_db_dest_path).connect()

    searchable_encryption(srcEngineClient, src_table, srcTableClient, columns_list, master_key, srcConnectionCloud)

    print("Finished")


def include_column_hash(src_db_cloud_path, src_db_user_path, src_table):
    print("2+=+5")
    # Creating connection with client database
    srcEngineCloud = create_engine(src_db_cloud_path)
    connectionCloud= srcEngineCloud.connect()

    # Adding "_index" into table name of cloud database
    cloud_table = src_table + "_index"

    # Getting columns names of client database
    result = connectionCloud.execute(f"SELECT * FROM {cloud_table}")
    columns_list = list(result.keys())

    # Adding line_hash column
    query = "ALTER TABLE " + str(cloud_table) + " ADD line_hash TEXT"
    srcEngineCloud.execute(query)
    columns_list.append("line_hash")

    # create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    SourceSessionCloud = sessionmaker(srcEngineCloud)
    srcEngineCloud._metadata = MetaData(bind=srcEngineCloud)
    srcEngineCloud._metadata.reflect(srcEngineCloud)  # get columns from existing table
    srcEngineCloud._metadata.tables[cloud_table].columns = [
        i for i in srcEngineCloud._metadata.tables[f"{cloud_table}"].columns if (i.name in columns_list)]
    srcTableCloud= Table(cloud_table, srcEngineCloud._metadata)
    sourceSessionCloud = SourceSessionCloud()

    # Adding id column
    column = Column('id', Integer, primary_key=True, autoincrement=True)
    column.add(srcTableCloud)
    sourceSessionCloud.commit()
    
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

    print("2+=+5")

    return
    
    first_line = sourceSessionCloud.query(srcTableCloud).first()
    print(first_line)

    # First id available
    id = first_line.id

    size = 1000
    statement = select(srcTableUser)
    results_proxy = sourceSessionUser.execute(statement) # Proxy to get data on batch
    results = results_proxy.fetchmany(size) # Getting data

    while results:
        for result in results:
            print(result)

        results = results_proxy.fetchmany(size) # Getting data

    return

    query_client = sourceSessionClient.query(srcTableClient).all()

    query_user = sourceSessionUser.query(srcTableUser).all()

    print(query_client)