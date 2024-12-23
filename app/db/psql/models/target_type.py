from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from app.db.psql.models import Base

class TargetType(Base):
    __tablename__ = 'target_types'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)

    # Relationships
    events = relationship("Event", back_populates="target_type")
