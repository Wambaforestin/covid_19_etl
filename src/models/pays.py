from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from .base import Base

class Pays(Base):
    __tablename__ = 'pays'
    
    id_pays = Column(Integer, primary_key=True)
    nom_pays = Column(String(100), unique=True, nullable=False)
    region_oms = Column(String(50), nullable=False)
    
    situations = relationship("SituationPandemique", back_populates="pays")