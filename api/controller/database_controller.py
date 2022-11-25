from controller import app, db
from flask import jsonify, request
from model.database_key_model import DatabaseKey
from model.database_model import Database, database_share_schema, databases_share_schema
from model.valid_database_model import (
    ValidDatabase,
    valid_database_share_schema,
    valid_databases_share_schema,
)
from service.authenticate import jwt_required
from service.database_service import (
    get_columns_database_types,
    get_primary_key,
    show_database,
)
from service.rsa_service import generateKeys
from sqlalchemy import create_engine, inspect
from sqlalchemy_utils import database_exists


@app.route("/getDatabases", methods=["GET"])
@jwt_required
def getDatabases(current_user):
    try:
        id = current_user.id

        result_databases = databases_share_schema.dump(
            Database.query.filter_by(id_user=id).all()
        )

        result_valid_databases = valid_databases_share_schema.dump(
            ValidDatabase.query.all()
        )

        for database in result_databases:
            for type in result_valid_databases:
                if type["id"] == database["id_db_type"]:
                    database["name_db_type"] = type["name"]
    except:
        return jsonify({"message": "databases_invalid_data"}), 400

    return jsonify(result_databases), 200


@app.route("/addDatabase", methods=["POST"])
@jwt_required
def addDatabase(current_user):
    try:
        # Get user id
        id_user = current_user.id

        # Get clinet database information
        id_db_type = request.json.get("id_db_type")
        name = request.json.get("name")
        host = request.json.get("host")
        user = request.json.get("user")
        port = request.json.get("port")
        pwd = request.json.get("password")

        database = Database(id_user, id_db_type, name, host, user, port, pwd, "")
        db.session.add(database)
        db.session.flush()
    except:
        db.session.rollback()
        return jsonify({"message": "database_invalid_data"}), 400

    try:
        # Generate rsa keys and save them
        publicKeyStr, privateKeyStr = generateKeys()
        database_keys = DatabaseKey(database.id, publicKeyStr, privateKeyStr)
        db.session.add(database_keys)
    except:
        db.session.rollback()
        return jsonify({"message": "database_keys_not_added"}), 500

    # Commit updates
    db.session.commit()

    return jsonify({"message": "database_added"}), 201


@app.route("/deleteDatabase", methods=["DELETE"])
@jwt_required
def deleteDatabase(current_user):
    try:
        id_db = request.json.get("id_db")
        database = Database.query.filter_by(id=id_db).first()

        if not database:
            return jsonify({"message": "database_not_found"}), 404

        if database.id_user != current_user.id:
            return jsonify({"message": "user_unauthorized"}), 401

        DatabaseKey.query.filter_by(id_db=id_db).delete()

        db.session.delete(database)
        db.session.commit()

        result = database_share_schema.dump(Database.query.filter_by(id=id).first())

        if not result:
            return jsonify({"message": "database_deleted"}), 200
        else:
            return jsonify({"message": "database_not_deleted"}), 500
    except:
        return jsonify({"message": "database_not_deleted"}), 500


@app.route("/testConnectionDatabase", methods=["POST"])
@jwt_required
def test_connection(current_user):
    try:
        # Get id of database to encrypt
        id_db = request.json.get("id_db")

        # Get database path by id
        result_database = database_share_schema.dump(
            Database.query.filter_by(id=id_db).first()
        )

        if not result_database:
            src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
                request.json.get("type").lower(),
                request.json.get("user"),
                request.json.get("password"),
                request.json.get("host"),
                request.json.get("port"),
                request.json.get("name"),
            )

            engine = create_engine(src_client_db_path)

            if database_exists(engine.url):
                return jsonify({"message": "database_connected"}), 200

            return jsonify({"message": "database_not_connected"}), 409

        db_type_name = (
            ValidDatabase.query.filter_by(id=result_database["id_db_type"]).first().name
        )
        src_client_db_path = f"{db_type_name}://{result_database['user']}:{result_database['password']}@{result_database['host']}:{result_database['port']}/{result_database['name']}"

        # Create connection to database
        engine = create_engine(src_client_db_path)

        if database_exists(engine.url):
            return jsonify({"message": "database_connected"}), 200

        return jsonify({"message": "database_not_connected"}), 409
    except:
        return jsonify({"message": "database_not_connected"}), 409


@app.route("/showDatabase", methods=["POST"])
@jwt_required
def showDatabase(current_user):

    try:
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

        # Get database informations
        db_type_name = (
            ValidDatabase.query.filter_by(id=result_database["id_db_type"]).first().name
        )

        src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
            db_type_name,
            result_database["user"],
            result_database["password"],
            result_database["host"],
            result_database["port"],
            result_database["name"],
        )

        # Get data and show
        result_query = show_database(
            src_client_db_path=src_client_db_path,
            src_table=request.json.get("table"),
            page=request.json.get("page"),
            per_page=request.json.get("per_page"),
        )
    except:
        return jsonify({"message": "database_invalid_data"}), 400

    return jsonify(result_query), 200


@app.route("/tablesDatabase", methods=["POST"])
@jwt_required
def tablesDatabase(current_user):
    try:
        # Get id of database to encrypt
        id_db = request.json["id_db"]

        # Get database path by id
        result_database = database_share_schema.dump(
            Database.query.filter_by(id=id_db).first()
        )

        if not result_database:
            return jsonify({"message": "database_not_found"}), 404

        db_type_name = (
            ValidDatabase.query.filter_by(id=result_database["id_db_type"]).first().name
        )
        src_db_path = f"{db_type_name}://{result_database['user']}:{result_database['password']}@{result_database['host']}:{result_database['port']}/{result_database['name']}"

        # Create connection to database
        engine_db = create_engine(src_db_path)

        return jsonify({"tables": list(engine_db.table_names())}), 200
    except:
        return jsonify({"message": "database_invalid_data"}), 400


@app.route("/columnsDatabase", methods=["POST"])
@jwt_required
def columnsDatabase(current_user):
    # Get Database ID
    id_db = request.json.get("id_db")

    # Get table name
    table_name = request.json.get("table")

    columns, status_code, response_message = get_columns_database_types(
        id_db=id_db, table_name=table_name
    )

    if not columns:
        return jsonify({"message": response_message}), status_code

    return jsonify(columns), status_code


@app.route("/getPrimaryKey", methods=["GET"])
@jwt_required
def getPrimaryKey(current_user):
    try:
        # Get id of database to encrypt
        id_db = request.json.get("id_db")

        # Get database information by id
        result_database = database_share_schema.dump(
            Database.query.filter_by(id=id_db).first()
        )

        if not result_database:
            return jsonify({"message": "database_not_found"}), 404

        db_type_name = (
            ValidDatabase.query.filter_by(id=result_database["id_db_type"]).first().name
        )
        src_db_path = f"{db_type_name}://{result_database['user']}:{result_database['password']}@{result_database['host']}:{result_database['port']}/{result_database['name']}"

        # Get table name
        table_name = request.json.get("table")

        return jsonify({"primary_key": get_primary_key(src_db_path, table_name)}), 200
    except:
        return jsonify({"message": "database_invalid_data"}), 400


"""@ app.route('/copy_database', methods=['GET'])
def copy_database():

    src_db = request.json['src_db']
    src_db_path = "{}://{}:{}@{}:{}/{}".format(src_db['type'], src_db['user'],
                                               src_db['password'], src_db['ip'], src_db['port'], src_db['name'])
    src_table = src_db['table']

    dest_db = request.json['dest_db']
    dest_db_path = "{}://{}:{}@{}:{}/{}".format(dest_db['type'], dest_db['user'],
                                                dest_db['password'], dest_db['ip'], dest_db['port'], dest_db['name'])
    dest_table = dest_db['table']
    dest_columns = dest_db['columns']

    copy_database_fc(src_db_path, dest_db_path,
                     src_table, dest_columns, dest_table)
    create_model(dest_db_path, dest_table, dest_columns)

    return jsonify({
        'message': 'Database copied successfully!'
    })"""
