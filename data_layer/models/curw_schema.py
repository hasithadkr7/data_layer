from sqlalchemy import Column, VARCHAR, DATETIME, DECIMAL, INT

from data_layer.base import Base


class Data(Base):
    id = Column(VARCHAR(64), nullable=False, primary_key=True)
    time = Column(DATETIME, nullable=False, primary_key=True)
    value = Column(DECIMAL(8, 3), nullable=False)
    __tablename__ = 'data'

    def __repr__(self):
        return '<Data %r %r %r>' % (self.id, self.time, self.value)


class Run(Base):
    id = Column(VARCHAR(64), nullable=False, primary_key=True)
    name = Column(VARCHAR(255), nullable=False)
    start_date = Column(DATETIME)
    end_date = Column(DATETIME)
    station = Column(INT, nullable=False)
    variable = Column(INT, nullable=False)
    unit = Column(INT, nullable=False)
    type = Column(INT, nullable=False)
    source = Column(INT, nullable=False)
    __tablename__ = 'run'

    def __repr__(self):
        return '<RunView %r %r %r %r %r %r %r>' % \
               (self.id, self.name, self.station, self.variable, self.unit, self.type, self.source)


class RunView(Base):
    id = Column(VARCHAR(64), nullable=False, primary_key=True)
    name = Column(VARCHAR(255), nullable=False)
    start_date = Column(DATETIME)
    end_date = Column(DATETIME)
    station = Column(VARCHAR(45), nullable=False)
    variable = Column(VARCHAR(100), nullable=False)
    unit = Column(VARCHAR(10), nullable=False)
    type = Column(VARCHAR(45), nullable=False)
    source = Column(VARCHAR(45), nullable=False)
    __tablename__ = 'run_view'

    def __repr__(self):
        return '<RunView %r %r %r %r %r %r %r>' % \
               (self.id, self.name, self.station, self.variable, self.unit, self.type, self.source)
