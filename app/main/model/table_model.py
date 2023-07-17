from sqlalchemy import func

from app.main import db


class Table(db.Model):
    __tablename__ = "table"
    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    database_id = db.Column(db.Integer, db.ForeignKey("database.id"), nullable=False)

    name = db.Column(db.String(100), nullable=False)
    encryption_progress = db.Column(db.Integer, nullable=False, default=0)
    anonimyzation_progress = db.Column(db.Integer, nullable=False, default=0)
    create_at = db.Column(db.DateTime, server_default=func.now())
    update_at = db.Column(db.DateTime, onupdate=func.now())

    database = db.relationship("Database", back_populates="tables", lazy=True)
    anonymization_records = db.relationship(
        "AnonymizationRecord", back_populates="table"
    )

    @property
    def encrypted(self):
        return True if self.encryption_progress >= 100 else False

    @property
    def anonymized(self):
        return True if self.anonimyzation_progress >= 100 else False

    @property
    def remove_anonimyzation_progress(self):
        return 100 - self.anonimyzation_progress

    def __repr__(self):
        return f"<Table: {self.id}>"
