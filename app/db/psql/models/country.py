from app.db.psql.models import Base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

class Country(Base):
    __tablename__ = 'countries'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    region_id = Column(Integer, ForeignKey('regions.id'), nullable=True)

    # Relationships
    location = relationship("Location", back_populates="country",uselist=False)
    cities = relationship("City", back_populates="country")
    region = relationship("Region", back_populates="countries", uselist=False)
