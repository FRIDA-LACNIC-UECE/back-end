from controller import app, db
from flask import jsonify, request
from service.authenticate import jwt_required
from service.cloud_service import row_search


@app.route("/rowSearch", methods=["POST"])
@jwt_required
def rowSearch(current_user):
    # Get Database ID
    id_db = request.json.get("id_db")

    # Get table name to search
    table_name = request.json.get("table_name")

    # Get search type
    search_type = request.json.get("search_type")

    # Get search value
    search_value = request.json.get("search_value")

    # Search row
    found_row, status_code, response_message = row_search(
        id_db_user=current_user.id,
        id_db=id_db,
        table_name=table_name,
        search_type=search_type,
        search_value=search_value,
    )

    if not found_row:
        return jsonify({"message": response_message}), status_code

    return jsonify(found_row), status_code
