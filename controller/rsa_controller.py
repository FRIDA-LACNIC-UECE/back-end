from flask import jsonify, request
from sqlalchemy import create_engine

from controller import app, db

from model.database_model import Database, database_share_schema
from model.valid_database_model import ValidDatabase
from model.database_key_model import DatabaseKey, database_key_share_schema

from service.authenticate import jwt_required
from service import rsa_service


@ app.route('/encrypt_database', methods=['GET'])
@ jwt_required
def encrypt_database(current_user):
    # Get id of database to encrypt
    id_db = request.json['id_db']

    # Get database information by id
    result_database = database_share_schema.dump(
        Database.query.filter_by(id=id_db).first()
    )

    if not result_database:
        return jsonify({'message': 'Database não encontrado!'}), 404

    db_type_name = ValidDatabase.query.filter_by(id=result_database['id_db_type']).first().name
    src_client_db_path = f"{db_type_name}://{result_database['user']}:{result_database['password']}@{result_database['host']}:{result_database['port']}/{result_database['name']}"
    src_dest_db_path = f"{db_type_name}://{result_database['user']}:{result_database['password']}@{result_database['host']}:{result_database['port']}/{result_database['name']}_cloud"

    # Get public and private keys of database
    result_keys = database_key_share_schema.dump(
        DatabaseKey.query.filter_by(id_db=id_db).first()
    )

    # Get tables names
    engine_db = create_engine(src_client_db_path)
    tables_names = engine_db.table_names()

    # Encrypting database
    
    for table_name in tables_names:
        rsa_service.encrypt_database(
            src_client_db_path, src_dest_db_path, table_name, 
            1000, result_keys['public_key'], result_keys['private_key']
        )
    return jsonify({'message': 'Banco de dados criptografado com sucesso!'}), 200

       