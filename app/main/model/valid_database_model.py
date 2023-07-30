from sqlalchemy import func

from app.main import db


class ValidDatabase(db.Model):
    __tablename__ = "valid_database"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)

    name = db.Column(db.String(255), nullable=False, unique=True)
    dialect = db.Column(db.String(255), nullable=False, unique=True)
    created_at = db.Column(db.DateTime, server_default=func.now())
    updated_at = db.Column(db.DateTime, onupdate=func.now())

    databases = db.relationship("Database", back_populates="valid_database")

    def __repr__(self):
        return f"<Valid Database {self.name}>"
