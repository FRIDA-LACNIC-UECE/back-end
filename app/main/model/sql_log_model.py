from sqlalchemy import func

from app.main import db


class SqlLog(db.Model):
    __tablename__ = "sql_log"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    database_id = db.Column(db.Integer, db.ForeignKey("database.id"), nullable=False)
    sql_command = db.Column(db.String(1000), nullable=False)
    create_at = db.Column(db.DateTime, server_default=func.now())
    update_at = db.Column(db.DateTime, onupdate=func.now())

    database = db.relationship("Database", back_populates="sql_log", lazy=True)

    def __repr__(self):
        return f"<SqlCommand: {self.sql_command}, CreateAt: {self.create_at}, UpdateAt: {self.update_at}>"
