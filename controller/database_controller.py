from flask import jsonify, request
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists

from controller import app, db

from service.authenticate import jwt_required

from model.database_model import Database, DatabaseSchema, database_share_schema, databases_share_schema
from model.valid_database_model import ValidDatabase, ValidDatabaseSchema, valid_database_share_schema, valid_databases_share_schema
from model.database_key_model import DatabaseKey

from service.rsa_service import generateKeys, encrypt_database


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
        return jsonify({'message': 'Database deleted successfully!'})
    else:
        return jsonify({'error': 'Could\'nt delete database, please try again!'})
        