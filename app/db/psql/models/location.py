from sqlalchemy import Column, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from app.db.psql.models import Base

class Location(Base):
    __tablename__ = 'locations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    country_id = Column(Integer, ForeignKey('countries.id'), nullable=True)
    city_id = Column(Integer, ForeignKey('cities.id'), nullable=True)
    region_id = Column(Integer, ForeignKey('regions.id'), nullable=True)

    # Relationships with explicit foreign_keys
    event = relationship("Event", back_populates="location", uselist=False)
    country = relationship("Country", back_populates="location", uselist=False)
    city = relationship("City", back_populates="location", uselist=False)
    region = relationship("Region", back_populates="location", uselist=False)