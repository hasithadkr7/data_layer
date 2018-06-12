from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


def get_engine(host, port, user, password, db):
    db_url = "mysql+pymysql://%s:%s@%s:%d/%s" % (user, password, host, port, db)
    return create_engine(db_url)


def get_sessionmaker(engine):
    return sessionmaker(bind=engine)


# Base class for all the schema model classes
Base = declarative_base()
