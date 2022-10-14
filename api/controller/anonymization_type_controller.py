from flask import jsonify, request

from controller import app, db
from model.anonymization_type_model import AnonymizationType, anonymization_types_share_schema
from service.authenticate import jwt_required


@ app.route('/getAnonymizationType', methods=['GET'])
@ jwt_required
def getAnonymizationType(current_user):

    try:
        result = anonymization_types_share_schema.dump(
            AnonymizationType.query.all()
        )
    except:
        return jsonify({
            'message': 'anonymization_types_not_found'
        }), 404

    return jsonify(result)


@ app.route('/addAnonymizationType', methods=['POST'])
@ jwt_required
def addAnonymizationType(current_user):

    try:
        if current_user.is_admin != 1:
            return jsonify({
                'message': 'required_administrator_privileges'
            }), 403

        name = request.json.get('name')

        anonymization_type = AnonymizationType(name=name)
        db.session.add(anonymization_type)
        db.session.commit()
    except:
        return jsonify({
            'message': 'anonymization_type_invalid_data'
        }), 400

    return jsonify({
        'message': 'anonymization_type_added'
    }), 200


@ app.route('/deleteAnonymizationType', methods=['DELETE'])
@jwt_required
def deleteAnonymizationType(current_user):
    
    try:
        if current_user.is_admin != 1:
            return jsonify({
                'message': 'required_administrator_privileges'
            }), 403
            
        # Get anonymization type data
        id_anonymization_type = request.json.get('id_anonymization_type')
        anonymization_type = AnonymizationType.query.filter_by(id=id_anonymization_type).first()

        if anonymization_type == None:
            return jsonify({
                'message': 'anonymization_type_not_found'
            }), 404
    
        db.session.delete(anonymization_type)
        db.session.commit()
    except:
        return jsonify({
            'message': 'anonymization_type_not_deleted'
        }), 500

    return jsonify({'message': 'anonymization_type_deleted'}), 200
    