from config import HOST, PASSWORD_DATABASE, PORT, TYPE_DATABASE, USER_DATABASE
from controller import app, db
from flask import jsonify, request
from model.database_model import Database, database_share_schema
from model.valid_database_model import ValidDatabase
from service.authenticate import jwt_required
from service.sse_service import (
    delete_hash_rows,
    include_column_hash,
    include_hash_rows,
    show_hash_rows,
)


@app.route("/includeHashRows", methods=["POST"])
@jwt_required
def includeHashRows(current_user):
    try:
        # Get name current user
        name_current = current_user.name

        # Get id of database to encrypt
        id_db = request.json.get("id_db")

        # Get database information by id
        database = Database.query.filter_by(id=id_db).first()
        result_database = database_share_schema.dump(database)

        if not database:
            return jsonify({"message": "database_not_found"}), 404

        if database.id_user != current_user.id:
            return jsonify({"message": "user_unauthorized"}), 401

        src_db_cloud_path = "{}://{}:{}@{}:{}/{}".format(
            TYPE_DATABASE,
            USER_DATABASE,
            PASSWORD_DATABASE,
            HOST,
            PORT,
            f"{result_database['name']}_cloud",
        )

        include_hash_rows(
            src_db_cloud_path, request.json.get("table"), request.json.get("hash_rows")
        )

        return jsonify({"message": "hash_included"}), 200
    except:
        return jsonify({"message": "hash_not_included"}), 400


@app.route("/includeHashColumn", methods=["POST"])
@jwt_required
def includeColumnHash(current_user):
    try:
        # Get name current user
        name_current = current_user.name

        # Get id of database to encrypt
        id_db = request.json.get("id_db")

        # Get database information by id
        database = Database.query.filter_by(id=id_db).first()
        result_database = database_share_schema.dump(database)

        if not database:
            return jsonify({"message": "database_not_found"}), 404

        if database.id_user != current_user.id:
            return jsonify({"message": "user_unauthorized"}), 401

        db_type_name = (
            ValidDatabase.query.filter_by(id=result_database["id_db_type"]).first().name
        )

        src_db_user_path = "{}://{}:{}@{}:{}/{}".format(
            db_type_name,
            result_database["user"],
            result_database["password"],
            result_database["host"],
            result_database["port"],
            "UserDB",
        )

        src_db_cloud_path = "{}://{}:{}@{}:{}/{}".format(
            TYPE_DATABASE,
            USER_DATABASE,
            PASSWORD_DATABASE,
            HOST,
            PORT,
            f"{result_database['name']}_cloud",
        )

        include_column_hash(
            src_db_cloud_path, src_db_user_path, request.json.get("table")
        )

        return jsonify({"message": "hash_included"}), 200
    except:
        return jsonify({"message": "hash_not_included"}), 400


@app.route("/showHashRows", methods=["POST"])
@jwt_required
def showHashRows(current_user):

    # Get name current user
    name_current = current_user.name

    # Get id of database to encrypt
    id_db = request.json.get("id_db")

    # Get database information by id
    database = Database.query.filter_by(id=id_db).first()
    result_database = database_share_schema.dump(database)

    # Return error: database not found (404)
    if not database:
        return jsonify({"message": "database_not_found"}), 404

    # Return error: database does not belong to the user (401)
    if database.id_user != current_user.id:
        return jsonify({"message": "user_unauthorized"}), 401

    src_cloud_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE,
        USER_DATABASE,
        PASSWORD_DATABASE,
        HOST,
        PORT,
        f"{result_database['name']}_cloud",
    )

    # Get data and show
    (
        result_query,
        primary_key_value_min_limit,
        primary_key_value_max_limit,
    ) = show_hash_rows(
        src_cloud_db_path=src_cloud_db_path,
        src_table=request.json.get("table"),
        page=request.json.get("page"),
        per_page=request.json.get("per_page"),
    )

    return (
        jsonify(
            {
                "result_query": result_query,
                "primary_key_value_min_limit": primary_key_value_min_limit,
                "primary_key_value_max_limit": primary_key_value_max_limit,
            }
        ),
        200,
    )


@app.route("/deleteRowsHash", methods=["POST"])
@jwt_required
def deleteHashRows(current_user):

    # Get name current user
    name_current = current_user.name

    # Get id of database to encrypt
    id_db = request.json.get("id_db")

    # Get database information by id
    database = Database.query.filter_by(id=id_db).first()
    result_database = database_share_schema.dump(database)

    # Return error: database not found (404)
    if not database:
        return jsonify({"message": "database_not_found"}), 404

    # Return error: database does not belong to the user (401)
    if database.id_user != current_user.id:
        return jsonify({"message": "user_unauthorized"}), 401

    src_cloud_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE,
        USER_DATABASE,
        PASSWORD_DATABASE,
        HOST,
        PORT,
        f"{result_database['name']}_cloud",
    )

    primary_key_list = request.json.get("primary_key_list")

    if primary_key_list == None or len(primary_key_list) == 0:
        return jsonify({"message": "primary_key_list_invalid_data"}), 400

    # Get data and show
    result_query = delete_hash_rows(
        src_cloud_db_path=src_cloud_db_path,
        src_table=request.json.get("table"),
        primary_key_list=primary_key_list,
    )

    return jsonify({"message": "delete_rows_hash_deleted"}), 200
