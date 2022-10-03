from flask import jsonify, request

from controller import app, db
from model.database_model import Database, database_share_schema
from model.valid_database_model import ValidDatabase
from model.anonymization_model import Anonymization, anonymizations_share_schema
from service import anonymization_service
from service.authenticate import jwt_required


@ app.route('/getAnonymization', methods=['GET'])
@ jwt_required
def getAnonymization(current_user):

    result = anonymizations_share_schema.dump(
        Anonymization.query.all()
    )

    return jsonify(result)


@ app.route('/addAnonymization', methods=['POST'])
@jwt_required
def addAnonimization(current_user):
    # Get id of database to encrypt
    id_db = request.json['id_db']

    # Get anonymization type
    id_anonymization_type = request.json['id_anonymization_type']

    # Get chosen table
    table_name = request.json['table']

    # Get chosen columns
    columns_to_anonimization = request.json['columns']

    anonymization = Anonymization(
        id_database=id_db, id_anonymization_type=id_anonymization_type, 
        table=table_name, columns=columns_to_anonimization
    )

    db.session.add(anonymization)
    db.session.commit()

    return jsonify({
        'message': 'Anonymization added successfully!'
    }), 201


@ app.route('/anonymization_database', methods=['POST'])
@jwt_required
def anonymization_database(current_user):
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

    # Get chosen columns
    lists_columns_anonymizations = anonymizations_share_schema.dump(
        Anonymization.query.filter_by(id_database=id_db).all()
    )

    # Run anonymization
    anonymization_service.anonimization_database(src_client_db_path, lists_columns_anonymizations)

    print("\n\nAnonimizando\n\n")

    return jsonify({
        'message': 'Dados anonimizado com sucesso!'
    }), 200