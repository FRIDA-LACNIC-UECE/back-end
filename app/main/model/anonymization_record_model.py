from sqlalchemy import func

from app.main import db


class AnonymizationRecord(db.Model):
    __tablename__ = "anonymization_record"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    table_id = db.Column(db.Integer, db.ForeignKey("table.id"), nullable=False)
    anonymization_type_id = db.Column(
        db.Integer, db.ForeignKey("anonymization_type.id"), nullable=False
    )

    columns = db.Column(db.JSON, nullable=False)
    created_at = db.Column(db.DateTime, server_default=func.now())
    updated_at = db.Column(db.DateTime, onupdate=func.now())

    table = db.relationship("Table", back_populates="anonymization_records")
    anonymization_type = db.relationship(
        "AnonymizationType", back_populates="anonymization_records"
    )

    def __repr__(self):
        return f"<Anonymization Record: {self.id}>"
