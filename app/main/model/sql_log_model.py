from sqlalchemy import func

from app.main import db


class SqlLog(db.Model):
    __tablename__ = "sql_log"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    database_id = db.Column(db.Integer, db.ForeignKey("database.id"), nullable=False)

    sql_command = db.Column(db.String(1000), nullable=False)
    create_at = db.Column(db.DateTime, server_default=func.now())
    update_at = db.Column(db.DateTime, onupdate=func.now())

    database = db.relationship("Database", back_populates="sql_logs")

    def __repr__(self) -> str:
        return f"<Sql Log: database_id = {self.database_id} - sql_comand = {self.sql_command}>"
