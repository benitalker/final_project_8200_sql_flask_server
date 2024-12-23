from app.db.psql.models import Base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

class Region(Base):
    __tablename__ = 'regions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

    # Relationships
    countries = relationship("Country", back_populates="region")
    location = relationship("Location", back_populates="region",uselist=False)
