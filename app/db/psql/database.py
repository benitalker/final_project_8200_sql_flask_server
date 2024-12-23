import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv(verbose=True)
db_url = os.getenv("PSQL_URL")

engine = create_engine(db_url)
session_maker = sessionmaker(bind=engine)
