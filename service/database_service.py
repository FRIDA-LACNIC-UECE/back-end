import csv
import pandas as pd
from sqlalchemy import (MetaData, Table, create_engine, inspect)
from sqlalchemy.orm import sessionmaker, Session


def show_database(src_client_db_path, src_table, page=0, per_page=100):

    # Creating connection with original database
    engine_client_db = create_engine(src_client_db_path)
    session_original_db = Session(engine_client_db)

    # Create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    engine_client_db._metadata = MetaData(bind=engine_client_db)
    engine_client_db._metadata.reflect(engine_client_db) 

    # Get columns from existing table
    with engine_client_db.connect() as conn:
        result = conn.execute(f"SELECT * FROM {src_table} LIMIT 1")
    
    columns_list = list(result._metadata.keys)

    engine_client_db._metadata.tables[src_table].columns = [
        i for i in engine_client_db._metadata.tables[src_table].columns if (i.name in columns_list)]
    table_client_db = Table(src_table, engine_client_db._metadata)
    
    # Run paginate
    query = session_original_db.query(table_client_db)

    if per_page is not None:
        query = query.limit(per_page)
    if page is not None:
        query = query.offset(page*per_page)

    return [row._asdict() for row in query]


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

    query = sourceSession.query(srcTable)
    
    try:
        if not destTable.exists():
            destTable.create(checkfirst=True)
        destSession.execute('DELETE FROM {}'.format(dest_table))
        for row in query:
            destSession.execute(destTable.insert(row))
        destSession.commit()
    except Exception as e:
        print(e)


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