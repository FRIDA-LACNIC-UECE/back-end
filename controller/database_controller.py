from flask import jsonify, request
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists
from sqlalchemy import create_engine, inspect

from controller import app, db

from model.database_model import Database, database_share_schema, databases_share_schema
from model.valid_database_model import ValidDatabase, valid_database_share_schema, valid_databases_share_schema
from model.database_key_model import DatabaseKey

from service.authenticate import jwt_required
from service.rsa_service import generateKeys
from service.database_service import copy_database_fc, create_model


@ app.route('/getDatabases', methods=['GET'])
@ jwt_required
def getDatabases(current_user):

    id = current_user.id

    result = databases_share_schema.dump(
        Database.query.filter_by(id_user=id).all()
    )
    result2 = valid_databases_share_schema.dump(
        ValidDatabase.query.all()
    )
    for database in result:
        for type in result2:
            if type['id'] == database['id_db_type']:
                database['name_db_type'] = type['name']
    return jsonify(result)


@ app.route('/addDatabase', methods=['POST'])
@ jwt_required
def addDatabase(current_user):
    # Get user id
    id_user = current_user.id

    # Get clinet database information
    id_db_type = request.json['id_db_type']
    name = request.json['name']
    host = request.json['host']
    user = request.json['user']
    port = request.json['port']
    pwd = request.json['password']

    database = Database(id_user, id_db_type, name, host, user, port, pwd, '')
    db.session.add(database)
    db.session.flush()

    # Generate rsa keys and save them
    publicKeyStr, privateKeyStr = generateKeys()
    database_keys = DatabaseKey(database.id, publicKeyStr, privateKeyStr)
    db.session.add(database_keys)

    # Commit updates
    db.session.commit()

    return jsonify({
        'message': 'Database added successfully!'
    }), 201


@ app.route('/deleteDatabase', methods=['POST'])
@ jwt_required
def deleteDatabase(current_user):
    
    id = request.json['id']

    database = Database.query.filter_by(id=id).first()

    if not database:
        return jsonify({
            'error': 'Database doesn\'t exist, please try again!'
        }), 409

    db.session.delete(database)
    db.session.commit()

    result = database_share_schema.dump(
        Database.query.filter_by(id=id).first()
    )

    if not result:
        return jsonify({'message': 'Database deleted successfully!'}), 200
    else:
        return jsonify({'error': 'Could\'nt delete database, please try again!'})


@ app.route('/copy_database', methods=['GET'])
def copy_database():

    src_db = request.json['src_db']
    src_db_path = "{}://{}:{}@{}:{}/{}".format(src_db['type'], src_db['user'],
                                               src_db['password'], src_db['ip'], src_db['port'], src_db['name'])
    src_table = src_db['table']

    dest_db = request.json['dest_db']
    dest_db_path = "{}://{}:{}@{}:{}/{}".format(dest_db['type'], dest_db['user'],
                                                dest_db['password'], dest_db['ip'], dest_db['port'], dest_db['name'])
    dest_table = dest_db['table']
    dest_columns = dest_db['columns']

    copy_database_fc(src_db_path, dest_db_path,
                     src_table, dest_columns, dest_table)
    create_model(dest_db_path, dest_table, dest_columns)

    return jsonify({
        'message': 'Database copied successfully!'
    })


@app.route('/test_connection', methods=['POST'])
@jwt_required
def test_connection(current_user):
    try:
        # Get id of database to encrypt
        id_db = request.json['id_db']

        # Get database path by id
        result_database = database_share_schema.dump(
            Database.query.filter_by(id=id_db).first()
        )

        if not result_database:
            return jsonify({'message': 'Database não encontrado!'}), 404

        db_type_name = ValidDatabase.query.filter_by(id=result_database['id_db_type']).first().name
        src_client_db_path = f"{db_type_name}://{result_database['user']}:{result_database['password']}@{result_database['host']}:{result_database['port']}/{result_database['name']}"
        
        # Create connection to database
        engine = create_engine(src_client_db_path)

        if database_exists(engine.url):
            return jsonify({
                'message': 'Connection Successful!'
            }), 200
    except:
        return jsonify({
            'error': 'Cannot check if database exists!'
        }), 409


@app.route('/columnsDatabase', methods=['POST'])
@jwt_required
def columns_database(current_user):
    # Get id of database to encrypt
    id_db = request.json['id_db']

    # Get database information by id
    result_database = database_share_schema.dump(
        Database.query.filter_by(id=id_db).first()
    )

    if not result_database:
        return jsonify({'message': 'Database não encontrado!'}), 404

    db_type_name = ValidDatabase.query.filter_by(id=result_database['id_db_type']).first().name
    src_db_path = f"{db_type_name}://{result_database['user']}:{result_database['password']}@{result_database['host']}:{result_database['port']}/{result_database['name']}"

    # Get table name
    table_name = request.json['table']

    # Create connection to database
    engine_db = create_engine(src_db_path)

    # Get columns and their types
    columns = {}

    insp = inspect(engine_db)
    columns_table = insp.get_columns(table_name)

    for c in columns_table :
        columns[f"{c['name']}"] = str(c['type'])

    return jsonify(columns)


@app.route('/tablesDatabase', methods=['POST'])
@jwt_required
def tables_database(current_user):
    # Get id of database to encrypt
    id_db = request.json['id_db']

    # Get database path by id
    result_database = database_share_schema.dump(
        Database.query.filter_by(id=id_db).first()
    )

    if not result_database:
        return jsonify({'message': 'Database não encontrado!'}), 404

    db_type_name = ValidDatabase.query.filter_by(id=result_database['id_db_type']).first().name
    src_db_path = f"{db_type_name}://{result_database['user']}:{result_database['password']}@{result_database['host']}:{result_database['port']}/{result_database['name']}"

    # Create connection to database
    engine_db = create_engine(src_db_path)

    return {"tables": list(engine_db.table_names())}