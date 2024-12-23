from sqlalchemy import Column, Integer, String
from app.db.psql.models import Base

class TerroristGroup(Base):
    __tablename__ = 'terrorist_group'
    id = Column(Integer, primary_key=True, autoincrement=True)
    group_name = Column(String(255), unique=True, nullable=False)