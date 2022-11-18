import numpy as np
import pandas as pd
import scipy.linalg as la
from Crypto.Cipher import AES
from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.orm import sessionmaker
from faker import Faker

from service.database_service import get_primary_key, create_table_session


def anonymization_name(seed):
    Faker.seed(seed)
    faker = Faker(['pt_BR'])

    return faker.name()


def anonymization_data(primary_key_name, row_to_anonymize):
    keys_dictionary = list(row_to_anonymize.keys())
    keys_dictionary.remove(primary_key)

    for key in keys_dictionary:
        row_to_anonymize[key] = anonymization_name(
            seed=row_to_anonymize[primary_key_name]
        )
        
    return row_to_anonymize


def anonymization_database_rows(src_client_db_path, table_name, columns_to_anonymization, rows_to_anonymization):

    #Get primary key of Client Database
    primary_key = get_primary_key(src_client_db_path, table_name)

    # Get primary key of Client Database in columns_to_anonymization only query
    columns_to_anonymization.insert(0, primary_key)

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, 
        table_name=table_name, 
        columns_list=columns_to_anonymization
    )
    
    # Transform rows database to dataframe
    dataframe_to_anonymization = pd.DataFrame(
        data=rows_to_anonymization
    )
    dataframe_to_anonymization = dataframe_to_anonymization[columns_to_anonymization]
   
    # Remove primary key of columns_to_anonymization list
    # but save elements in save_primary_key_elements
    save_primary_key_elements = dataframe_to_anonymization[primary_key]
    dataframe_to_anonymization = dataframe_to_anonymization.drop(primary_key, axis=1)
    columns_to_anonymization.remove(primary_key)

    # Run anonymization
    anonymization_dataframe = dataframe_to_anonymization[columns_to_anonymization]
    #anonymization_dataframe = anonymization_data(anonymization_dataframe)
    anonymization_dataframe = pd.DataFrame(
        data=anonymization_dataframe, 
        columns=columns_to_anonymization
    )
    
    # Reorganize primary key elements
    anonymization_dataframe[f"{primary_key}"] = save_primary_key_elements

    # Get anonymized data to update
    dictionary_anonymized_data = anonymization_dataframe.to_dict(orient='records')
    print(dictionary_anonymized_data)

    # Update data
    for row_anonymized in dictionary_anonymized_data:
        session_client_db.query(table_client_db).\
        filter(table_client_db.c[0] == row_anonymized[f'{primary_key}']).\
        update(row_anonymized)

    session_client_db.commit()
    session_client_db.close()


def anonymization_database(src_client_db_path, table_name, columns_to_anonymization):

    #Get primary key of Client Database
    primary_key = get_primary_key(src_client_db_path, table_name)

    # Get primary key of Client Database in columns_to_anonymization only query
    columns_to_anonymization.insert(0, primary_key)

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, 
        table_name=table_name,
        columns_list=columns_to_anonymization
    )
        
    # Get data to dataframe
    size = 1000
    statement = select(table_client_db)
    results_proxy = session_client_db.execute(statement) # Proxy to get data on batch
    results = results_proxy.fetchmany(size) # Getting data

    while results:
        result = [row._asdict() for row in results]

        print(result)
        
        for row_number in range(0, len(result)):
            result[row_number] = anonymization_data(
                primary_key_name=primary_key,
                row_to_anonymize=result[row_number]
            )
        
        # Update data
        for row_anonymized in result:
            session_client_db.query(table_client_db).\
            filter(table_client_db.c[0] == row_anonymized[f'{primary_key}']).\
            update(row_anonymized)

        session_client_db.commit()
    
        results = results_proxy.fetchmany(size) # Getting data

        break

    session_client_db.close()

    return



if __name__ == '__main__':

    src_client_db_path = "mysql://root:Dd16012018@localhost:3306/test_db"
    table_name = 'nivel1'
    columns_to_anonymization = ["nome"]

    # Get primary key of Client Database
    primary_key = get_primary_key(src_client_db_path, table_name)

    # Get primary key of Client Database in columns_to_anonymization only query
    #columns_to_anonymization.insert(0, primary_key)

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_client_db_path, table_name
    )

    # Get data in database
    rows_to_anonymization = session_client_db.query(table_client_db).all()

    anonymization_database(
        src_client_db_path,
        table_name,
        columns_to_anonymization
    )