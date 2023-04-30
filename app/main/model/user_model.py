from sqlalchemy import func

from app.main import db


class User(db.Model):
    __tablename__ = "user"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    username = db.Column(db.String(100), nullable=False, unique=True)
    email = db.Column(db.String(100), nullable=False, unique=True)
    password = db.Column(db.String(1000), nullable=False)
    is_admin = db.Column(db.Integer, nullable=False, server_default="0")
    created_at = db.Column(db.DateTime, server_default=func.now())
    updated_at = db.Column(db.DateTime, onupdate=func.now())

    database = db.relationship("Database", back_populates="user", lazy=True)

    def __repr__(self) -> str:
        return f"<User {self.username}>"
