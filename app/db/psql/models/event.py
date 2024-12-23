from sqlalchemy import Column, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from app.db.psql.models import Base

class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=True)
    month = Column(Integer, nullable=True)
    day = Column(Integer, nullable=True)
    summary = Column(String, nullable=True)
    success = Column(Boolean, nullable=True)
    suicide = Column(Boolean, nullable=True)
    attack_type_id = Column(Integer, ForeignKey('attack_types.id'), nullable=True)
    target_type_id = Column(Integer, ForeignKey('target_types.id'), nullable=True)
    casualties_id = Column(Integer, ForeignKey('casualties.id'), nullable=True)
    location_id = Column(Integer, ForeignKey('locations.id'), nullable=True)
    group_id = Column(Integer, ForeignKey('terrorist_group.id'),nullable=True)

    # Relationships
    attack_type = relationship("AttackType", back_populates="events")
    target_type = relationship("TargetType", back_populates="events")
    casualties = relationship("Casualties", back_populates="event")
    location = relationship("Location", back_populates="event", uselist=False)
    group = relationship("TerroristGroup", backref="events")
