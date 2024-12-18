from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dsi.backends.filesystem import Filesystem

class SqlAlchemy(Filesystem):
    filename = "sqlite:///fs.db"
    engine = None

    def __init__(self, filename, base):
        self.filename = filename
        self.engine = create_engine(filename, echo=True)
        base.metadata.create_all(self.engine)

    def put_artifacts(self, artifact_list):
        with Session(self.engine) as session:
            session.add_all(artifact_list)
            session.commit()

    def query(self, stmt):
        results = []
        with Session(self.engine) as session:
            for obj in session.scalars(stmt):
                results.append(obj)

        return results
    
    def close(self):
        if self.engine:
            self.engine.dispose()
