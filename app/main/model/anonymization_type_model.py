from sqlalchemy import func

from app.main import db


class AnonymizationType(db.Model):
    __tablename__ = "anonymization_type"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    name = db.Column(db.String(255), nullable=False, unique=True)
    created_at = db.Column(db.DateTime, server_default=func.now())
    updated_at = db.Column(db.DateTime, onupdate=func.now())

    anonymization_records = db.relationship(
        "AnonymizationRecord", back_populates="anonymization_type"
    )

    def __repr__(self):
        return f"<AnonymizationType: {self.name}>"
