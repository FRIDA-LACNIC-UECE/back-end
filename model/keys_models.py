from controller import db, ma
from flask_login import UserMixin


class DatabaseKey(db.Model, UserMixin):
    __tablename__ = 'databases_keys'

    id = db.Column(db.Integer, nullable=False,
                   autoincrement=True, primary_key=True)
    id_db = db.Column(db.Integer, db.ForeignKey(
        "databases.id"), nullable=False)
    public_key = db.Column(db.String(200), nullable=False)
    private_key = db.Column(db.String(200), nullable=False)
    
    def __repr__(self):
        return f'<id_db : {self.name}, public_key: {self.public_key, }private_key : {self.private_key}>'


class DatabaseKeySchema(ma.Schema):
    class Meta:
        fields = ('id', 'id_db', 'public_key', 'private_key',
                  'host', 'user', 'port', 'password', 'ssh')


databases_key_share_schema = DatabaseKeySchema()
databases_keys_share_schema = DatabaseKeySchema(many=True)