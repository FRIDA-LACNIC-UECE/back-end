import csv
import pandas as pd
from sqlalchemy import (MetaData, Table, create_engine, inspect)
from sqlalchemy.orm import sessionmaker, Session

from model.valid_database_model import ValidDatabase
from model.database_model import Database, database_share_schema


def get_path_database(id_db):
    # Get database path by id and check if database found exist
    result_database = database_share_schema.dump(
        Database.query.filter_by(id=id_db).first()
    )

    if not result_database:
        return None
    
    db_type_name = ValidDatabase.query.filter_by(
        id=result_database['id_db_type']
    ).first().name
    
    # Define Client Database Path
    src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
        db_type_name, result_database['user'], result_database['password'],
        result_database['host'], result_database['port'],
        result_database['name']
    )

    return src_client_db_path


def get_columns_database(engine_db, table_name):
    columns_list = []
    insp = inspect(engine_db)
    columns_table = insp.get_columns(table_name)

    for c in columns_table :
        columns_list.append(str(c['name']))

    return columns_list


def create_table_session(src_db_path, table_name, columns_list=None):
    # Create engine, reflect existing columns
    engine_db = create_engine(src_db_path)
    engine_db._metadata = MetaData(bind=engine_db)

    # Get columns from existing table
    engine_db._metadata.reflect(engine_db)
    
    if columns_list == None:
        columns_list = get_columns_database(engine_db, table_name)

    engine_db._metadata.tables[table_name].columns = [
        i for i in engine_db._metadata.tables[table_name].columns if (i.name in columns_list)
    ]
    
    # Create table object of Client Database
    table_object_db = Table(table_name, engine_db._metadata)

    # Create session of Client Database to run sql operations
    session_db = Session(engine_db)

    return table_object_db, session_db


def get_primary_key(src_db_path, table_name):

    # Create table object of database
    table_object_db, _ = create_table_session(src_db_path, table_name)
    
    return [key.name for key in inspect(table_object_db).primary_key][0]


def get_index_column_table_object(src_db_path, table_name, column_name):
    
    # Create table object of database
    table_object_db, _ = create_table_session(src_db_path, table_name)

    # Search index of column
    index = 0
    for column in table_object_db.c:
        if column.name == column_name:
            return index
        index += 1

    return None


def show_database(src_db_path, table_name, page=0, per_page=100):

    table_object_db, session_db = create_table_session(src_db_path, table_name)
    
    # Run paginate
    query = session_db.query(table_object_db)

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