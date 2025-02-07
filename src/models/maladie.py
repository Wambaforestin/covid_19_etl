from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import relationship
from .base import Base

class Maladie(Base):
    __tablename__ = 'maladie'
    
    id_maladie = Column(Integer, primary_key=True)
    nom_maladie = Column(String(50), unique=True, nullable=False)
    type_maladie = Column(String(50), nullable=False)
    description = Column(Text)
    
    situations = relationship("SituationPandemique", back_populates="maladie")