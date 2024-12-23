from sqlalchemy import Column, Integer, Boolean, Float
from sqlalchemy.orm import relationship
from app.db.psql.models import Base

class Casualties(Base):
    __tablename__ = 'casualties'

    id = Column(Integer, primary_key=True, autoincrement=True)
    killed = Column(Integer, default=0)
    wounded = Column(Integer, default=0)
    property_damage = Column(Boolean, nullable=True)
    property_value = Column(Float, nullable=True)

    # Relationships
    event = relationship("Event", back_populates="casualties", uselist=False)
