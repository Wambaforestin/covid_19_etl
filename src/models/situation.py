from sqlalchemy import Column, Integer, Date, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base

class SituationPandemique(Base):
    __tablename__ = 'situation_pandemique'
    
    id_pays = Column(Integer, ForeignKey('pays.id_pays'), primary_key=True)
    id_maladie = Column(Integer, ForeignKey('maladie.id_maladie'), primary_key=True)
    date_observation = Column(Date, primary_key=True)
    
    cas_confirmes = Column(Integer, default=0)
    deces = Column(Integer, default=0)
    guerisons = Column(Integer, default=0)
    cas_actifs = Column(Integer, default=0)
    nouveaux_cas = Column(Integer, default=0)
    nouveaux_deces = Column(Integer, default=0)
    nouvelles_guerisons = Column(Integer, default=0)
    
    pays = relationship("Pays", back_populates="situations")
    maladie = relationship("Maladie", back_populates="situations")