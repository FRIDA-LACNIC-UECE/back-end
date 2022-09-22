from sqlalchemy import (MetaData, Table, create_engine, inspect)
from sqlalchemy.orm import sessionmaker

from model.models import (AppMeta)

import numpy as np
import scipy.linalg as la
import csv
import pandas as pd


# create_model('mysql://root:Admin538*@localhost:3306/public',
#             'anonymizations', ['id', 'id_database', 'id_anonymization_type', 'table', 'columns'])
def create_model(db, table, columns):

    types = {
        'INTEGER': 'Integer',
        'VARCHAR': 'String',
        'TINYINT': 'Boolean'
    }

    engine = create_engine(db)
    meta = MetaData(bind=engine)
    table_name = table  # request.json['table']
    columns_to_copy = columns  # request.json['columns']
    table = Table(table_name, meta,
                  autoload=True, autoload_with=engine)
    insp = inspect(engine)
    columns_table = insp.get_columns(table_name)
    indexes_table = insp.get_indexes(table_name)
    f = open('demoModel.py', 'w')

    s = ''

    s += 'from app import db, ma\n'
    s += 'from flask_login import UserMixin\n'
    s += 'from werkzeug.security import check_password_hash, generate_password_hash\n\n'
    s += f'class {table_name}(db.Model, UserMixin):\n'
    s += f'\t__tablename__ = "{table_name}"\n'
    columns = ''
    for c in table.c:
        if(not c.name in columns_to_copy):
            continue
        type = str(c.type)
        if(len(columns)):
            columns += ', '
        columns += f'{c.name}'
        for word, initial in types.items():
            type = type.replace(word, initial)
        s2 = f'\t{c.name} = db.Column(db.{type}'
        if(c.server_default):
            s2 += f', default={c.server_default}'
        if(c.nullable == False):
            s2 += f', nullable={c.nullable}'
        if(c.autoincrement == True):
            s2 += f', autoincrement={c.autoincrement}'
        if(c.primary_key == True):
            s2 += f', primary_key={c.primary_key}'
        if([index for index in indexes_table if index['name'] == c.name]):
            s2 += f', unique=True'
        s2 += ')\n\n'
        s += s2
    s += f'\tdef __init__(self, {columns}):\n'
    for c in columns.split(', '):
        s += f'\t\tself.{c} = {c}\n'
    s += '\n\tdef __repr__(self):\n'
    s += f'\t\treturn f"<{table_name} : '
    for idx, c in enumerate(columns.split(', ')):
        if(idx):
            s += f', {{self.{c}}}'
        else:
            s += f'{{self.{c}}}'
    s += '>"'
    f.write(s)
    return 'Model Created'


def copy_database_fc(src_db_path, dest_db_path, src_table, dest_columns, dest_table):

    # create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    srcEngine = create_engine(src_db_path)
    SourceSession = sessionmaker(srcEngine)
    srcEngine._metadata = MetaData(bind=srcEngine)
    srcEngine._metadata.reflect(srcEngine)  # get columns from existing table
    srcEngine._metadata.tables[src_table].columns = [
        i for i in srcEngine._metadata.tables[src_table].columns if (i.name in dest_columns)]
    srcTable = Table(src_table, srcEngine._metadata)

    # create engine and table object for newTable
    # change this for your destination database
    destEngine = create_engine(dest_db_path)
    DestSession = sessionmaker(destEngine)
    destEngine._metadata = MetaData(bind=destEngine)
    destTable = Table(dest_table, destEngine._metadata)

    sourceSession = SourceSession()
    destSession = DestSession()

    # copy schema and create newTable from oldTable
    for column in srcTable.columns:
        if(column.name in dest_columns):
            destTable.append_column(column._copy())

    query = sourceSession.query(
        AppMeta.id, AppMeta.email, AppMeta.cpf).all()

    try:
        if not destTable.exists():
            destTable.create(checkfirst=True)
        destSession.execute('DELETE FROM {}'.format(dest_table))
        for row in query:
            destSession.execute(destTable.insert(row))
        destSession.commit()
    except Exception as e:
        print(e)


def anonimization_data(src_db_client_path, src_table, columns_to_anonimization):

    # create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    srcEngineClient = create_engine(src_db_client_path)
    SourceSessionClient = sessionmaker(srcEngineClient)
    srcEngineClient._metadata = MetaData(bind=srcEngineClient)
    srcEngineClient._metadata.reflect(srcEngineClient)  # get columns from existing table
    srcEngineClient._metadata.tables[src_table].columns = [
        i for i in srcEngineClient._metadata.tables[src_table].columns if (i.name in columns_to_anonimization)]
    srcTableClient = Table(src_table, srcEngineClient._metadata)

    sourceSessionClient = SourceSessionClient()
    
    query_client = sourceSessionClient.query(srcTableClient).all()
    #query_user = sourceSessionUser.query(srcTableUser).all()

    result = pd.DataFrame(data=query_client, columns=columns_to_anonimization)

    save_id = result['id']
    
    print(result)

    result = result.drop('id', axis=1)

    columns_to_anonimization.remove('id')

    result_anomization = result[columns_to_anonimization].astype('int')

    result_anomization = result_anomization.to_numpy()

    result_anomization = anonimization(result_anomization)
    
    result[columns_to_anonimization] = pd.DataFrame(data=result_anomization, columns=columns_to_anonimization)

    result.index = save_id

    #result_user = pd.DataFrame(data=query_user, columns=['id', 'line_hash'])
    #result['line_hash'] = result_user['line_hash']

    print(result)

    dictionary_update_data = result.to_dict(orient='records')

    first_line = sourceSessionClient.query(srcTableClient).first()
    id = first_line.id

    print(first_line)

    '''query = "ALTER TABLE " + str(destTable) + " ADD line_hash TEXT"
    srcEngineClient.execute(query)
    columns_to_anonimization.append("line_hash")
    print("Add line_hash")'''

    sourceSessionClient.commit()
    sourceSessionClient.close()

    for row_update in dictionary_update_data:
        print(row_update)
        sourceSessionClient.query(srcTableClient).\
        filter(srcTableClient.c[0] == id).\
        update(row_update)
        id += 1

    sourceSessionClient.commit()

    '''try:
        if not destTable.exists():
            destTable.create(checkfirst=True)
        destSession.execute('DELETE FROM {}'.format(dest_table))
        destSession.commit()
        with destEngine.begin() as connection:
            result.to_sql(dest_table, con=connection, if_exists='replace')
    except Exception as e:
        print(e)'''


def anonimization(data):
    # calculate the mean of each column
    mean = np.array(np.mean(data, axis=0).T)

    # center data
    data_centered = data - mean

    # calculate the covariance matrix
    cov_matrix = np.cov(data_centered, rowvar=False)

    # calculate the eignvalues and eignvectors
    evals, evecs = la.eigh(cov_matrix)

    # sort them
    idx = np.argsort(evals)[::-1]

    # Each columns of this matrix is an eingvector
    evecs = evecs[:, idx]
    evals = evals[idx]

    # explained variance
    variance_retained = np.cumsum(evals)/np.sum(evals)

    # calculate the transformed data
    data_transformed = np.dot(evecs.T, data_centered.T).T

    # randomize eignvectors
    new_evecs = evecs.copy().T
    for i in range(len(new_evecs)):
        np.random.shuffle(new_evecs[i])
    new_evecs = np.array(new_evecs).T

    # go back to the original dimension
    data_original_dimension = np.dot(data_transformed, new_evecs.T)
    data_original_dimension += mean

    return data_original_dimension


def csv_to_sql():

    # Create engine to connect with DB
    try:
        engine = create_engine(
            'mysql://root:Admin538*@localhost:3306/public')
    except:
        print("Can't create 'engine")

    # Get data from CSV file to DataFrame(Pandas)
    with open('fake_db.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        columns = [
            '_0',
            '_1',
            '_2',
            '_3',
            '_4',
            '_5',
            '_6',
            '_7',
            '_8',
            '_9'
        ]
        df = pd.DataFrame(data=reader, columns=columns)

    # Standart method of Pandas to deliver data from DataFrame to PastgresQL
    try:
        with engine.begin() as connection:
            df.to_sql('client', con=connection,
                      index_label='id', if_exists='replace')
            print('Done, ok!')
    except:
        print('Something went wrong!')
