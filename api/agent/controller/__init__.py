from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from flask_cors import CORS

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:larces@db-container:3306/public'
app.config['SQLALCHEMY_BINDS'] = {
    'db2': 'mysql://root:larces@db-container:3306/flask_jwt_appmeta',
    'db3': 'mysql://root:larces@db-container:3306/flask_jwt_maindb',
    'db4': 'mysql://root:larces@db-container:3306/flask_jwt_users'
}

app.config['SECRET_KEY'] = 'secret'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

db = SQLAlchemy(app)
ma = Marshmallow(app)
CORS(app)
