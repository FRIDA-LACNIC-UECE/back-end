from flask import jsonify, request

from controller import app, db

from service.authenticate import jwt_required

from model.valid_database_model import ValidDatabase, ValidDatabaseSchema, valid_database_share_schema, valid_databases_share_schema


@ app.route('/getValidDatabases', methods=['GET'])
@ jwt_required
def getValidDatabases(current_user):

    result = valid_databases_share_schema.dump(
        ValidDatabase.query.all()
    )

    return jsonify(result)


@ app.route('/addValidDatabase', methods=['POST'])
@ jwt_required
def addValidDatabase(current_user):
    name = request.json['name']

    valid_database = ValidDatabase(name=name)
    db.session.add(valid_database)
    db.session.commit()

    return jsonify({
        'message': 'Valid database added successfully!'
    })
    