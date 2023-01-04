from flask import jsonify, request

from api import app, db

from ..model import ValidDatabase, valid_databases_share_schema
from ..service.authenticate import jwt_required


@app.route("/getValidDatabases", methods=["GET"])
@jwt_required
def getValidDatabases(current_user):
    try:
        result = valid_databases_share_schema.dump(ValidDatabase.query.all())

        return jsonify(result), 200
    except:
        return jsonify({"message": "valid_databases_invalid_data"}), 400


@app.route("/addValidDatabase", methods=["POST"])
@jwt_required
def addValidDatabase(current_user):
    try:
        name = request.json.get("name")

        valid_database = ValidDatabase(name=name)
        db.session.add(valid_database)
        db.session.commit()

        return jsonify({"message": "valid_database_added"}), 200
    except:
        return jsonify({"message": "valid_database_invalid_data"}), 400
