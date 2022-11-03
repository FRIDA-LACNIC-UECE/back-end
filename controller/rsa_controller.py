from flask import jsonify, request
from sqlalchemy import create_engine

from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT
)

from controller import app, db

from model.database_model import Database, database_share_schema
from model.valid_database_model import ValidDatabase
from model.database_key_model import DatabaseKey, database_key_share_schema

from service.authenticate import jwt_required
from service import rsa_service


@ app.route('/encryptDatabaseRows', methods=['POST'])
@ jwt_required
def encryptDatabaseRows(current_user):
    # Get name current user
    name_current = current_user.name

    # Get id of database to encrypt
    id_db = request.json.get('id_db')

    # Get database information by id
    database = Database.query.filter_by(id=id_db).first()
    result_database = database_share_schema.dump(database)

    if not database:
        return jsonify({'message': 'database_not_found'}), 404

    if database.id_user != current_user.id:
        return jsonify({'message': 'user_unauthorized'}), 401

    db_type_name = ValidDatabase.query.filter_by(
        id=result_database['id_db_type']
    ).first().name

    src_dest_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, f"{result_database['name']}_cloud"
    )

    # Rows to encrypt
    rows_to_encrypt = request.json.get('rows_to_encrypt')
    if not rows_to_encrypt:
        return jsonify({'message': 'database_invalid_data'}), 400

    # Get tables names
    table_name = request.json.get('table')

    # Get public and private keys of database
    result_keys = database_key_share_schema.dump(
        DatabaseKey.query.filter_by(id_db=id_db).first()
    )

    if not result_keys:
        return jsonify({'message': 'database_keys_not_found'}), 404

    # Run cryptography
    rsa_service.encrypt_database_rows(
        rows_to_encrypt, src_dest_db_path, table_name,
        result_keys['public_key'], result_keys['private_key']
    )
    
    return jsonify({'message': 'database_rows_encrypted'}), 200


@ app.route('/encryptDatabase', methods=['POST'])
@ jwt_required
def encryptDatabase(current_user):
    try:
        # Get name current user
        name_current = current_user.name

        # Get id of database to encrypt
        id_db = request.json.get('id_db')

        # Get database information by id
        database = Database.query.filter_by(id=id_db).first()
        result_database = database_share_schema.dump(database)

        if not database:
            return jsonify({'message': 'database_not_found'}), 404

        if database.id_user != current_user.id:
            return jsonify({'message': 'user_unauthorized'}), 401

        db_type_name = ValidDatabase.query.filter_by(
            id=result_database['id_db_type']
        ).first().name

        src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
            db_type_name, result_database['user'], result_database['password'],
            result_database['host'], result_database['port'], result_database['name']
        )

        src_dest_db_path = "{}://{}:{}@{}:{}/{}".format(
            TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
            HOST, PORT, f"{result_database['name']}_cloud"
        )

        # Get tables names
        table_name = request.json.get('table')

        # Get columns to encrypted
        columns_list = request.json.get('columns')

        # Get public and private keys of database
        result_keys = database_key_share_schema.dump(
            DatabaseKey.query.filter_by(id_db=id_db).first()
        )

        if not result_keys:
            return jsonify({'message': 'database_keys_not_found'}), 404

        # Run cryptography
        rsa_service.encrypt_database(
            src_client_db_path, src_dest_db_path, table_name, columns_list,
            1000, result_keys['public_key'], result_keys['private_key']
        )
        
        return jsonify({'message': 'database_encrypted'}), 200
    except:
        return jsonify({'message': 'database_invalid_data'}), 400
