from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base() 

class GermanVocabulary(Base):
    __tablename__ = 'german_vocabulary'
    id = Column(Integer, primary_key=True)
    german_word = Column(String(100))
    french_word = Column(String(100))
    category = Column(String(100))
    number_char = Column(Integer)
    frequency = Column(Float)
    similarity_score = Column(Float)
