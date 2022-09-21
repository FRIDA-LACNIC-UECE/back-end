import base64
import imp
from multiprocessing.dummy import connection
import re

import rsa
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.orm import Session



def generateKeys():
    # Generating keys
    (publicKey, privateKey) = rsa.newkeys(2048)

    # Save keys on chosen path
    with open('./api/keys/publicKey.pem', 'wb') as p:
        p.write(publicKey.save_pkcs1('PEM'))
    with open('./api/keys/privateKey.pem', 'wb') as p:
        p.write(privateKey.save_pkcs1('PEM'))


def loadKeys():
    # Load keys on chosen path
    with open('./api/keys/publicKey.pem', 'rb') as p:
        publicKey = rsa.PublicKey.load_pkcs1(p.read())
    with open('./api/keys/privateKey.pem', 'rb') as p:
        privateKey = rsa.PrivateKey.load_pkcs1(p.read())

    return privateKey, publicKey


def encrypt(message, key):
    # Encrypt message
    encrypted_message = rsa.encrypt(message.encode(), key)

    # Convert from byte to base64
    base64_encrypted_message = base64.b64encode(encrypted_message)

    return str(base64_encrypted_message)[2:-1] # Remove caracter "b'" [0-1] and "'"[-1] of byte format.


def decrypt(encrypted_message, key):
   
    # Convert from string (base64) to bytes
    ciphertext = base64.b64decode(encrypted_message.encode())

    return rsa.decrypt(ciphertext, key).decode()


def encrypt_list(data_list, key):

    encrypted_list = []

    for data in data_list:
        data = str(data)
        encrypted_list.append(encrypt(data, key))

    return encrypted_list


def encrypt_database(src_original_db_path, src_dest_db_path, src_table, columns_list, size_batch):

    privateKey, publicKey = loadKeys()

    # Creating connection with original database
    engine_original_db = create_engine(src_original_db_path)
    session_original_db = Session(engine_original_db)

    # Getting columns name
    data_columns = engine_original_db.execute(f"SELECT * FROM {src_table} LIMIT 1")
    columns_list = list(data_columns.keys())

    print(columns_list)

    # Create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    engine_original_db._metadata = MetaData(bind=engine_original_db)
    engine_original_db._metadata.reflect(engine_original_db)  # get columns from existing table
    engine_original_db._metadata.tables[src_table].columns = [
        i for i in engine_original_db._metadata.tables[src_table].columns if (i.name in columns_list)]
    table_original_db = Table(src_table, engine_original_db._metadata)

    # Creating connection with dest database
    connection_dest_db = create_engine(src_dest_db_path).connect()

    # Check if id is a database column
    if "id" in columns_list:
        # Create engine, reflect existing columns, and create table object for oldTable
        # change this for your source database
        engine_original_db._metadata = MetaData(bind=engine_original_db)
        engine_original_db._metadata.reflect(engine_original_db)  # get columns from existing table
        engine_original_db._metadata.tables[src_table].columns = [
            i for i in engine_original_db._metadata.tables[src_table].columns if (i.name in columns_list)]
        table_original_db = Table(src_table, engine_original_db._metadata)

        statement = select(table_original_db)
        results_proxy = session_original_db.execute(statement) # Proxy to get data on batch
        results = results_proxy.fetchmany(size_batch) # Getting data

        # Getting index of column id
        index_id = columns_list.index('id')

        while results:
            
            from_db = []

            for result in results:
                # Transform result from tuple to list
                #data_list = list(result)

                # Remove id into data_list
                #del columns_list[index_id]

                #encrypted_list = encrypt_list(data_list, publicKey)

                from_db.append(list(result))


            # Create dataframe with data original database
            dataframe_db = pd.DataFrame(from_db, columns=columns_list)

            # Create name_columns without id
            names_columns = columns_list.copy()
            names_columns.remove('id')

            # Encrypt each column
            for column in names_columns:
                dataframe_db[column] = encrypt_list(list(dataframe_db[column]), publicKey)

            # Add column hash
            dataframe_db['line_hash'] = [None] * len(dataframe_db)    

            # Send data to database
            dataframe_db.to_sql(src_table, connection_dest_db, if_exists='append', index=False)
    
            results = results_proxy.fetchmany(size_batch) # Getting next data
    else:
        # Adding id on columns list
        columns_list.insert(0, "id")

        # Create engine, reflect existing columns, and create table object for oldTable
        # change this for your source database
        engine_original_db._metadata = MetaData(bind=engine_original_db)
        engine_original_db._metadata.reflect(engine_original_db)  # get columns from existing table
        engine_original_db._metadata.tables[src_table].columns = [
            i for i in engine_original_db._metadata.tables[src_table].columns if (i.name in columns_list)]
        table_original_db = Table(src_table, engine_original_db._metadata)

        statement = select(table_original_db)
        results_proxy = session_original_db.execute(statement) # Proxy to get data on batch
        results = results_proxy.fetchmany(size_batch) # Getting data

        id = 0

        while results:
            
            from_db = []

            for result in results:
                data_list = list(result)

                encrypted_list = encrypt_list(data_list, publicKey)
                encrypted_list.insert(0, id)

                from_db.append(encrypted_list)

                id += 1

            encrypted_dataframe = pd.DataFrame(from_db, columns=columns_list)
            print(encrypted_dataframe)

            encrypted_dataframe['line_hash'] = [None] * len(encrypted_dataframe)    
    
            encrypted_dataframe.to_sql(src_table, connection_dest_db, if_exists='append', index=False)
    
            results = results_proxy.fetchmany(size_batch) # Getting data


if __name__ == "__main__":
    generateKeys()

    privateKey, publicKey = loadKeys()

    message = "O RSA só é capaz de criptografar dados em uma quantidade máxima igual ao tamanho de sua chave (2048 bits = 256 bytes), menos qualquer preenchimento e dados de cabeçalho (11 bytes para preenchimento PKCS#1 v1.5)."

    print(f"Message: {message}\n")

    encrypted_message = encrypt(message, publicKey)
    print(f"Encrypted Message2: {encrypted_message}")
    print(f"Encrypted Message2.2: {type(encrypted_message)}\n")

    des_message = decrypt(encrypted_message, privateKey)
    print(f"des_message: {des_message}\n")