import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv


load_dotenv()

models_path = os.getenv("MODELS_PATH")

if models_path:
    sys.path.append(models_path)
    
from models.german_vocabulary import Base

engine = create_engine(os.environ["DB_NAME"], echo=True)
Base.metadata.create_all(engine)


print("La table 'german_vocabulary' a été créée avec succès.")
