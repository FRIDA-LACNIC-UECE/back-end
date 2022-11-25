from flask import jsonify, request
from sqlalchemy import create_engine

from controller import app, db
from model.anonymization_model import Anonymization, anonymizations_share_schema
from model.database_model import Database, database_share_schema
from model.valid_database_model import ValidDatabase
from service.anonymization_service import (
    anonymization_database,
    anonymization_database_rows,
)
from service.authenticate import jwt_required


@app.route("/getAnonymization", methods=["GET"])
@jwt_required
def getAnonymization(current_user):
    try:
        result = anonymizations_share_schema.dump(Anonymization.query.all())

        return jsonify(result)
    except:
        return jsonify({"message": "anonymizations_not_found"}), 404


@app.route("/addAnonymization", methods=["POST"])
@jwt_required
def addAnonymization(current_user):
    try:
        # Get database id to anonymize
        id_db = request.json.get("id_db")

        # Get anonymization type
        id_anonymization_type = request.json.get("id_anonymization_type")

        # Get name table to anonymize
        table_name = request.json.get("table")

        # Get chosen columns to anonymize
        columns_to_anonimization = request.json.get("columns")

        # Add anonymization
        anonymization = Anonymization(
            id_database=id_db,
            id_anonymization_type=id_anonymization_type,
            table=table_name,
            columns=columns_to_anonimization,
        )

        db.session.add(anonymization)
        db.session.commit()

        return jsonify({"message": "anonymization_created"}), 201

    except:
        return jsonify({"message": "anonymization_invalid_data"}), 400


@app.route("/deleteAnonymization", methods=["DELETE"])
@jwt_required
def deleteAnonymization(current_user):
    try:
        # Get anonymization id
        id_anonymization = request.json.get("id_anonymization")
        anonymization = Anonymization.query.filter_by(id=id_anonymization).first()

        if anonymization == None:
            return jsonify({"message": "anonymization_not_found"}), 404

        db.session.delete(anonymization)
        db.session.commit()

        return jsonify({"message": "anonymization_deleted"}), 200

    except:
        return jsonify({"message": "anonymization_not_deleted"}), 400


@app.route("/anonymizationDatabaseRows", methods=["POST"])
@jwt_required
def anonymizationDatabaseRows(current_user):

    # Get id of database to anonymize.
    id_db = request.json.get("id_db")

    # Get table_name to encrypt.
    table_name = request.json.get("table_name")

    # Get row of Client Database to anonymization.
    rows_to_anonymization = request.json.get("rows_to_anonymization")

    # Flag to indicate if anonymized rows will be inserted or returned.
    insert_database = request.json.get("insert_database")

    # Run anonymization.
    rows_to_anonymization, status_code, response = anonymization_database_rows(
        id_db=id_db,
        table_name=table_name,
        rows_to_anonymization=rows_to_anonymization,
        insert_database=insert_database,
    )

    if not rows_to_anonymization:
        return jsonify({"message": response}), status_code

    return jsonify(rows_to_anonymization), status_code


@app.route("/anonymizationDatabase", methods=["POST"])
@jwt_required
def anonymizationDatabase(current_user):

    # Get id of database to anonymize
    id_db = request.json.get("id_db")

    # Run anonymization
    status_code, response = anonymization_database(id_db=id_db)

    return jsonify({"message": response}), status_code


@app.route("/getColumnsToAnonymize", methods=["POST"])
@jwt_required
def getColumnsToAnonymize(current_user):

    try:
        # Get id of database to encrypt
        id_db = request.json.get("id_db")

        # Get database path by id
        result_database = database_share_schema.dump(
            Database.query.filter_by(id=id_db).first()
        )

        if not result_database:
            return jsonify({"message": "database_not_found"}), 404

        db_type_name = (
            ValidDatabase.query.filter_by(id=result_database["id_db_type"]).first().name
        )

        src_client_db_path = f"{db_type_name}://{result_database['user']}:{result_database['password']}@{result_database['host']}:{result_database['port']}/{result_database['name']}"

        # Get chosen columns
        lists_columns_anonymizations = anonymizations_share_schema.dump(
            Anonymization.query.filter_by(id_database=id_db).all()
        )

        sensitive_columns = []
        ids_type_anonymization = []

        # Get sensitive_columns
        for sensitive_column in lists_columns_anonymizations:
            sensitive_columns += sensitive_column["columns"]
            ids_type_anonymization += [sensitive_column["id_anonymization_type"]] * len(
                sensitive_column["columns"]
            )

        return (
            jsonify(
                {
                    "sensitive_columns": sensitive_columns,
                    "lists_columns_anonymizations": lists_columns_anonymizations,
                }
            ),
            200,
        )

    except Exception as e:
        print(e)
        return jsonify({"message": "anonymization_invalid_data"}), 400