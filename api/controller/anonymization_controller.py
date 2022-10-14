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

    try:
        result = anonymizations_share_schema.dump(
            Anonymization.query.all()
        )
    except:
        return jsonify({
            'message': 'anonymizations_not_found'
        }), 404

    return jsonify(result)


@ app.route('/addAnonymization', methods=['POST'])
@jwt_required
def addAnonymization(current_user):

    try:
        # Get id of database to encrypt
        id_db = request.json.get('id_db')

        # Get anonymization type
        id_anonymization_type = request.json.get('id_anonymization_type')

        # Get chosen table
        table_name = request.json.get('table')

        # Get chosen columns
        columns_to_anonimization = request.json.get('columns')
 
        anonymization = Anonymization(
            id_database=id_db, id_anonymization_type=id_anonymization_type, 
            table=table_name, columns=columns_to_anonimization
        )

        db.session.add(anonymization)
        db.session.commit()
    except:
        return jsonify({
            'message': 'anonymization_invalid_data'
        }), 400

    return jsonify({
        'message': 'anonymization_created'
    }), 201


@ app.route('/deleteAnonymization', methods=['DELETE'])
@jwt_required
def deleteAnonymization(current_user):
    
    try:
        # Get anonymization data
        id_anonymization = request.json.get('id_anonymization')
        anonymization = Anonymization.query.filter_by(id=id_anonymization).first()

        if anonymization == None:
            return jsonify({
                'message': 'anonymization_not_found'
            }), 404
        
        db.session.delete(anonymization)
        db.session.commit()
    except:
        return jsonify({
            'message': 'anonymization_not_deleted'
        }), 400

    return jsonify({'message': 'anonymization_deleted'})
    

@ app.route('/anonymization_database', methods=['POST'])
@jwt_required
def anonymization_database(current_user):

    try:
        # Get id of database to encrypt
        id_db = request.json.get('id_db')

        # Get database path by id
        result_database = database_share_schema.dump(
            Database.query.filter_by(id=id_db).first()
        )

        db_type_name = ValidDatabase.query.filter_by(id=result_database['id_db_type']).first().name

        src_client_db_path = f"{db_type_name}://{result_database['user']}:{result_database['password']}@{result_database['host']}:{result_database['port']}/{result_database['name']}"
    except:
        return jsonify({'message': 'database_not_found'}), 404

    try:
        # Get chosen columns
        lists_columns_anonymizations = anonymizations_share_schema.dump(
            Anonymization.query.filter_by(id_database=id_db).all()
        )

        # Run anonymization
        anonymization_service.anonimization_database(src_client_db_path, lists_columns_anonymizations)
    except:
        return jsonify({
            'message': 'anonymization_invalid_data'
        }), 400

    return jsonify({
        'message': 'anonymization_done'
    }), 200