from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import envs


DATABASE_URL = (
    f"postgresql://{envs.POSTGRES_USER}:{envs.POSTGRES_PASSWORD}"
    f"@{envs.POSTGRES_HOST}/{envs.POSTGRES_DB}"
)


engine = create_engine(
    DATABASE_URL,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
