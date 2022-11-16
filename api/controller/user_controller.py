import datetime

import jwt
from flask import jsonify, request

from controller import app, db
from service.authenticate import jwt_required
from model.user_model import User, user_share_schema, users_share_schema


@ app.route('/register', methods=['POST'])
def register():
    try:
        name = request.json.get('name')
        email = request.json.get('email')
        pwd = request.json.get('password')
        is_admin = False

        user = User.query.filter_by(email=email).first()

        if user:
            return jsonify({'message': 'user_already_registered'}), 409

        user = User(name, email, pwd, is_admin)
        db.session.add(user)
        db.session.commit()
    except:
        return jsonify({'message': 'user_invalid_data'}), 400

    return jsonify({'message': 'user_registered'}), 200


@ app.route('/login', methods=['POST'])
def login():
    try:
        email = request.json.get('email')
        pwd = request.json.get('password')

        user = User.query.filter_by(email=email).first()

        if not user or not user.verify_password(pwd):
            return jsonify({"message": "user_incorrect_data"}), 403

        payload = {
            "id": user.id,
            "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=240)
        }

        token = jwt.encode(payload, app.config['SECRET_KEY'])
    except:
        return jsonify({'message': 'user_invalid_data'}), 400

    return jsonify({
        'message': 'user_logged',
        'is_admin': user.is_admin,
        'token': token
    })


@ app.route('/getUser', methods=['GET'])
@ jwt_required
def getUser(current_user):
    try:
        return jsonify(user_share_schema.dump(current_user)), 200
    except:
        return jsonify({'message': 'user_invalid_data'}), 400
    


@ app.route('/getUsers', methods=['GET'])
@ jwt_required
def getUsers(current_user):
    
    try:
        if current_user.is_admin != 1:
            return jsonify({
                'message': 'required_administrator_privileges'
            }), 403

        result = users_share_schema.dump(
            User.query.all()
        )
    except:
        return jsonify({'message': 'users_invalid_data'}), 400

    return jsonify(result)


@ app.route('/deleteUser', methods=['DELETE'])
@ jwt_required
def deleteUser(current_user):
    
    try:
        if current_user.is_admin != 1:
            return jsonify({
                'message': 'required_administrator_privileges'
            }), 403

        email = request.json.get('email')

        user = User.query.filter_by(email=email).first()

        if not user:
            return jsonify({
                'message': 'user_not_found'
            }), 404

        db.session.delete(user)
        db.session.commit()

        result = user_share_schema.dump(
            User.query.filter_by(email=email).first()
        )
    except:
        return jsonify({'message': 'user_invalid_data'}), 400

    if not result:
        return jsonify({'message': 'user_deleted'}), 200
    else:
        return jsonify({'message': 'user_not_deleted'}), 500

'''
@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))
'''
