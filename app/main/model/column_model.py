from sqlalchemy import func

from app.main import db


class Column(db.Model):
    __tablename__ = "column"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    table_id = db.Column(db.Integer, db.ForeignKey("table.id"), nullable=False)
    anonymization_type_id = db.Column(db.Integer, nullable=False)
    name = db.Column(db.String(1000), nullable=False)
    type = db.Column(db.String(1000), nullable=False)
    create_at = db.Column(db.DateTime, server_default=func.now())
    update_at = db.Column(db.DateTime, onupdate=func.now())

    table = db.relationship("Table", back_populates="columns")

    def __repr__(self):
        return f"<TableId: {self.table_id }, AnonimyzationTypeId: {self.anonimyzation_type_id}, Name: {self.name}, Type: {self.type}, CreateAt: {self.create_at}, UpdateAt: {self.update_at}>"
