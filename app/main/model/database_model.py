from app.main import db


class Database(db.Model):
    __tablename__ = "database"
    _table_args_ = (
        db.UniqueConstraint(
            "name",
            "username",
            "host",
            "port",
            name="unique_database_name_username_host_port",
        ),
    )

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    valid_database_id = db.Column(
        db.Integer, db.ForeignKey("valid_database.id"), nullable=False
    )
    name = db.Column(db.String(100), nullable=False)
    host = db.Column(db.String(100), nullable=False)
    username = db.Column(db.String(100), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    password = db.Column(db.String(100), nullable=False)
    ssh = db.Column(db.String(100))

    user = db.relationship("User", back_populates="database", lazy=True)
    valid_database = db.relationship(
        "ValidDatabase", back_populates="database", lazy=True
    )
    anonymization_record = db.relationship(
        "AnonymizationRecord", back_populates="database", lazy=True
    )
    database_key = db.relationship("DatabaseKey", back_populates="database", lazy=True)
    sql_log = db.relationship("SqlLog", back_populates="database", lazy=True)

    def __repr__(self):
        return f"<Database : {self.name}>"
