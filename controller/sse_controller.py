from flask import jsonify, request

from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT
)

from model.database_model import Database, database_share_schema
from model.valid_database_model import ValidDatabase

from controller import app, db

from service.authenticate import jwt_required
from service.sse_service import include_column_hash, show_hash_rows
from service import sse_service



@ app.route('/includeColumnHash', methods=['POST'])
@ jwt_required
def includeColumnHash(current_user):
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

        src_db_user_path = "{}://{}:{}@{}:{}/{}".format(
            db_type_name, result_database['user'], result_database['password'],
            result_database['host'], result_database['port'], "UserDB"
        )

        src_db_cloud_path = "{}://{}:{}@{}:{}/{}".format(
            TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
            HOST, PORT, f"{result_database['name']}_cloud"
        )

        include_column_hash(src_db_cloud_path, src_db_user_path, request.json.get('table'))
        
        return jsonify({'message': 'hash_included'}), 200
    except:
        return jsonify({'message': 'hash_not_included'}), 400


@app.route('/showHashRows', methods=['POST'])
@jwt_required
def showHashRows(current_user):

    # Get name current user
    name_current = current_user.name

    # Get id of database to encrypt
    id_db = request.json.get('id_db')

    # Get database information by id
    database = Database.query.filter_by(id=id_db).first()
    result_database = database_share_schema.dump(database)

    # Return error: database not found (404)
    if not database:
        return jsonify({'message': 'database_not_found'}), 404

    # Return error: database does not belong to the user (401)
    if database.id_user != current_user.id:
        return jsonify({'message': 'user_unauthorized'}), 401

    src_cloud_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, f"{result_database['name']}_cloud"
    )

    # Get data and show
    result_query = show_hash_rows(
        src_cloud_db_path=src_cloud_db_path, 
        src_table=request.json.get('table'),
        page=request.json.get('page'), per_page=request.json.get('per_page')
    )

    return jsonify(result_query), 200


@ app.route('/row_by_hash', methods=['GET'])
def line_by_hash():
    try:
        src_db_cloud = request.json.get('src_db_cloud')
        src_db_cloud_path = "{}://{}:{}@{}:{}/{}".format(src_db_cloud['type'], src_db_cloud['user'],
                                                src_db_cloud['password'], src_db_cloud['ip'], src_db_cloud['port'], src_db_cloud['name'])
        
        table_name = src_db_cloud["table"]

        hash = request.json.get('line_hash')

        row_found = sse_service.line_by_hash(src_db_cloud_path, table_name, hash)
    except:
        return jsonify({'message': 'row_invalid_data'}), 400

    return jsonify({'row_found': row_found}), 200


@ app.route('/row_by_id', methods=['GET'])
def line_by_id():
    try:
        src_db_cloud = request.json.get('src_db_cloud')
        src_db_cloud_path = "{}://{}:{}@{}:{}/{}".format(src_db_cloud['type'], src_db_cloud['user'],
                                                src_db_cloud['password'], src_db_cloud['ip'], src_db_cloud['port'], src_db_cloud['name'])
        
        table_name = src_db_cloud["table"]

        id = request.json.get('id')

        row_found = sse_service.line_by_id(src_db_cloud_path, table_name, id)
    except:
        return jsonify({'message': 'row_invalid_data'}), 400

    return jsonify({'row_found': row_found}), 200