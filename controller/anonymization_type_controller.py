from flask import jsonify, request

from controller import app, db
from model.anonymization_type_model import AnonymizationType, anonymization_types_share_schema
from service.authenticate import jwt_required


@ app.route('/getAnonymizationType', methods=['GET'])
@ jwt_required
def getAnonymizationType(current_user):

    result = anonymization_types_share_schema.dump(
        AnonymizationType.query.all()
    )

    return jsonify(result)


@ app.route('/addAnonymizationType', methods=['POST'])
@ jwt_required
def addAnonymizationType(current_user):
    name = request.json['name']

    anonymization_type = AnonymizationType(name=name)
    db.session.add(anonymization_type)
    db.session.commit()

    return jsonify({
        'message': 'Anonymization type added successfully!'
    })