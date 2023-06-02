from sqlalchemy import func

from app.main import db

STATUS_OPTIONS = ["active", "wait_activation", "blocked"]


class User(db.Model):
    __tablename__ = "user"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)

    username = db.Column(db.String(30), nullable=False, unique=True)
    email = db.Column(db.String(255), nullable=False, unique=True)
    password = db.Column(db.String(255), nullable=False)
    status = db.Column(
        db.Enum(*STATUS_OPTIONS, name="status_enum"), server_default="wait_activation"
    )
    is_admin = db.Column(db.Boolean, nullable=False, default=False)
    activation_token = db.Column(db.String(256), nullable=True)
    activation_token_exp = db.Column(db.Integer, nullable=True)
    reset_password_token = db.Column(db.String(256), nullable=True)
    reset_password_token_exp = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, server_default=func.now())
    updated_at = db.Column(db.DateTime, onupdate=func.now())

    databases = db.relationship("Database", back_populates="user")

    def __repr__(self) -> str:
        return f"<User {self.username}>"
