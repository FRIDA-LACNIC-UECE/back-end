from flask import jsonify, request

from controller import app, db

from service.authenticate import jwt_required
from service import sse_service


@ app.route('/include_column_hash', methods=['GET'])
def include_column_hash():
    try:
        src_db_cloud = request.json.get('src_db_cloud')
        src_db_cloud_path = "{}://{}:{}@{}:{}/{}".format(src_db_cloud['type'], src_db_cloud['user'],
                                                src_db_cloud['password'], src_db_cloud['ip'], src_db_cloud['port'], src_db_cloud['name'])

        src_table = src_db_cloud['table']

        src_db_user = request.json.get('src_db_user')
        src_db_user_path = "{}://{}:{}@{}:{}/{}".format(src_db_user['type'], src_db_user['user'],
                                                src_db_user['password'], src_db_user['ip'], src_db_user['port'], src_db_user['name'])

        sse_service.include_column_hash(src_db_cloud_path, src_db_user_path, src_table)
    except:
        return jsonify({'message': 'hash_not_included'}), 400

    return jsonify({'message': 'hash_included'}), 200


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