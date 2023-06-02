from sqlalchemy import func

from app.main import db


class AnonymizationRecord(db.Model):
    __tablename__ = "anonymization_record"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    database_id = db.Column(db.Integer, db.ForeignKey("database.id"), nullable=False)
    anonymization_type_id = db.Column(
        db.Integer, db.ForeignKey("anonymization_type.id"), nullable=False
    )

    table = db.Column(db.String(255), nullable=False)
    columns = db.Column(db.JSON, nullable=False)
    created_at = db.Column(db.DateTime, server_default=func.now())
    updated_at = db.Column(db.DateTime, onupdate=func.now())

    database = db.relationship("Database", back_populates="anonymization_records")
    anonymization_type = db.relationship(
        "AnonymizationType", back_populates="anonymization_records"
    )

    def __repr__(self):
        return f"<AnonymizationRecord: {self.database_id} - {self.anonymization_type_id} - {self.table} - {self.columns}>"
