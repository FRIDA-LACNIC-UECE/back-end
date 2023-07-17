from sqlalchemy import func

from app.main import db


class Database(db.Model):
    __tablename__ = "database"
    __table_args__ = (
        db.UniqueConstraint(
            "valid_database_id",
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
    created_at = db.Column(db.DateTime, server_default=func.now())
    updated_at = db.Column(db.DateTime, onupdate=func.now())

    user = db.relationship("User", back_populates="databases")
    valid_database = db.relationship("ValidDatabase", back_populates="databases")
    tables = db.relationship("Table", back_populates="database")
    database_key = db.relationship("DatabaseKey", back_populates="database")
    sql_logs = db.relationship("SqlLog", back_populates="database")

    @property
    def url(self) -> str:
        return "{}://{}:{}@{}:{}/{}".format(
            self.valid_database.dialect,
            self.username,
            self.password,
            self.host,
            self.port,
            self.name,
        )

    def __repr__(self):
        return f"<Database: {self.id}>"
