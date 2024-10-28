from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

class EnglishVocabularyTable(Base):

    __tablename__ = "english_vocabulary"
    id = Column(Integer, primary_key=True)
    english_word = Column(String(100))
    french_word = Column(String(100))
    category = Column(String(100))
    number_char = Column(Integer)
    frequency = Column(Float)
    similarity_score = Column(Float)
    difficulty = Column(Integer)
