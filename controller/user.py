import datetime

import jwt
from flask import jsonify, request

from controller import app, db
from service.authenticate import jwt_required
from model.user_model import User, user_share_schema, users_share_schema


@ app.route('/register', methods=['POST'])
def register():
    name = request.json['name']
    email = request.json['email']
    pwd = request.json['password']
    is_admin = False

    user = User.query.filter_by(email=email).first()

    if user:
        return jsonify({
            'error': 'User already registered, please try again!'
        }), 409

    user = User(name, email, pwd, is_admin)
    db.session.add(user)
    db.session.commit()

    return jsonify({
        'message': 'User registered successfully!'
    })


@ app.route('/login', methods=['POST'])
def login():
    email = request.json['email']
    pwd = request.json['password']

    user = User.query.filter_by(email=email).first()

    if not user or not user.verify_password(pwd):
        return jsonify({
            "error": "Incorrect data, please try again!"
        }), 403

    payload = {
        "id": user.id,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=60)
    }

    token = jwt.encode(payload, app.config['SECRET_KEY'])

    return jsonify({
        'message': 'Logged successfully!',
        'is_admin': user.is_admin,
        'token': token
    })

@ app.route('/getUser', methods=['GET'])
@ jwt_required
def getUser(current_user):
    return jsonify(user_share_schema.dump(current_user))


@ app.route('/getUsers', methods=['GET'])
@ jwt_required
def getUsers(current_user):
    result = users_share_schema.dump(
        User.query.all()
    )
    return jsonify(result)


@ app.route('/deleteUser', methods=['POST'])
@ jwt_required
def deleteUser(current_user):

    email = request.json['email']

    user = User.query.filter_by(email=email).first()

    if not user:
        return jsonify({
            'error': 'User does not exist, please try again!'
        }), 409

    db.session.delete(user)
    db.session.commit()

    result = user_share_schema.dump(
        User.query.filter_by(email=email).first()
    )

    if not result:
        return jsonify({'message': 'User deleted successfully!'})
    else:
        return jsonify({'error': 'Unable to delete user, please try again!'})
