from flask import jsonify, request

from controller import app, db

from service.authenticate import jwt_required
from service import sse_service


@ app.route('/include_column_hash', methods=['GET'])
def include_column_hash():

    src_db_cloud = request.json['src_db_cloud']
    src_db_cloud_path = "{}://{}:{}@{}:{}/{}".format(src_db_cloud['type'], src_db_cloud['user'],
                                               src_db_cloud['password'], src_db_cloud['ip'], src_db_cloud['port'], src_db_cloud['name'])

    src_table = src_db_cloud['table']

    src_db_user = request.json['src_db_user']
    src_db_user_path = "{}://{}:{}@{}:{}/{}".format(src_db_user['type'], src_db_user['user'],
                                               src_db_user['password'], src_db_user['ip'], src_db_user['port'], src_db_user['name'])

    sse_service.include_column_hash(src_db_cloud_path, src_db_user_path, src_table)

    return jsonify({
        'message': 'Incluido coluna hash com sucesso!'
    })


@ app.route('/line_by_hash', methods=['GET'])
def line_by_hash():

    src_db_cloud = request.json['src_db_cloud']
    src_db_cloud_path = "{}://{}:{}@{}:{}/{}".format(src_db_cloud['type'], src_db_cloud['user'],
                                               src_db_cloud['password'], src_db_cloud['ip'], src_db_cloud['port'], src_db_cloud['name'])
    
    table_name = src_db_cloud["table"]

    hash = request.json['line_hash']

    row_found = sse_service.line_by_hash(src_db_cloud_path, table_name, hash)

    return jsonify({
        'message': 'Pesquisa por hash concluída com sucesso!',
        'row_found': row_found
    })


@ app.route('/line_by_id', methods=['GET'])
def line_by_id():

    src_db_cloud = request.json['src_db_cloud']
    src_db_cloud_path = "{}://{}:{}@{}:{}/{}".format(src_db_cloud['type'], src_db_cloud['user'],
                                               src_db_cloud['password'], src_db_cloud['ip'], src_db_cloud['port'], src_db_cloud['name'])
    
    table_name = src_db_cloud["table"]

    id = request.json['id']

    row_found = sse_service.line_by_id(src_db_cloud_path, table_name, id)

    return jsonify({
        'message': 'Pesquisa por hash concluída com sucesso!',
        'row_found': row_found
    })