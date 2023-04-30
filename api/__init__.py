from flask import Flask
from flask_cors import CORS
from flask_marshmallow import Marshmallow
from flask_sqlalchemy import SQLAlchemy

from api.config import (
    HOST,
    NAME_DATABASE,
    PASSWORD_DATABASE,
    PORT,
    TYPE_DATABASE,
    USER_DATABASE,
)

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "{}://{}:{}@{}:{}/{}".format(
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, HOST, PORT, NAME_DATABASE
)
app.config["SECRET_KEY"] = "secret"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True

db = SQLAlchemy(app)
ma = Marshmallow(app)
CORS(app)
