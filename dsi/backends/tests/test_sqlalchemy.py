import git
from collections import OrderedDict

from dsi.backends.sqlalchemy import SqlAlchemy
from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy import select
import os
import subprocess
import csv

isVerbose = True

class Base(DeclarativeBase):
    pass


class Wildfire(Base):
    __tablename__ = "wildfire"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    files: Mapped[List["File"]] = relationship(
        back_populates="wildfire", cascade="all, delete-orphan"
    )
    wind_speed: Mapped[float]
    wdir: Mapped[int]
    smois: Mapped[float]
    fuels: Mapped[str]
    ignition: Mapped[str]
    safe_unsafe_ignition_pattern: Mapped[str]
    safe_unsafe_fire_behavior: Mapped[str]
    does_fire_meet_objectives: Mapped[str]
    rationale_if_unsafe: Mapped[Optional[str]]
    burned_area: Mapped[int]
    def __repr__(self) -> str:
        return f"Wildfire(id={self.id!r})"

class File(Base):
    __tablename__ = "file"

    id: Mapped[int] = mapped_column(primary_key=True)
    wildfire_id: Mapped[int] = mapped_column(ForeignKey("wildfire.id"))
    path: Mapped[str]
    wildfire: Mapped["Wildfire"] = relationship(back_populates="files")
    def __repr__(self) -> str:
        return f"File(id={self.id!r}, artifact_id={self.wildfire_id!r}, path={self.path!r})"

def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)


def test_wildfire_data_sql_artifact():
    engine_path = "sqlite:///wildfire.db"
    store = SqlAlchemy(engine_path, Base)
    store.close()
    
    # No error implies success
    assert True

def test_wildfire_data_csv_artifact():
    csvpath = '/'.join([get_git_root('.'), 'examples/wildfire/wildfiredata.csv'])
    engine_path = "sqlite:///wildfire.db"
    store = SqlAlchemy(engine_path, Base)
    print(csvpath)
    with open(csvpath) as csv_file:
        print(csvpath)
        csv_reader = csv.reader(csv_file, delimiter=',')
        header = next(csv_reader)
        artifacts = []
        for row in csv_reader:
            row_zipped = zip(header, row)
            row_dict = dict(row_zipped)
            wildfire_row = Wildfire(
                wind_speed=row_dict['wind_speed'],
                wdir=row_dict['wdir'],
                smois=row_dict['smois'],
                fuels=row_dict['fuels'],
                ignition=row_dict['ignition'],
                safe_unsafe_ignition_pattern=row_dict['safe_unsafe_ignition_pattern'],
                safe_unsafe_fire_behavior=row_dict['safe_unsafe_fire_behavior'],
                does_fire_meet_objectives=row_dict['does_fire_meet_objectives'],
                rationale_if_unsafe=row_dict['rationale_if_unsafe'],
                burned_area=row_dict['burned_area'],
                files=[File(path=row_dict['FILE'])]
            )
            print(row)
            artifacts.append(wildfire_row)
    store.put_artifacts(artifacts)
    store.close()
    
    # No error implies success
    assert True

def test_wildfire_artifact_query():
    engine_path = "sqlite:///wildfire.db"
    store = SqlAlchemy(engine_path, Base)
    stmt = select(Wildfire).where(Wildfire.burned_area > 188000)
    results = store.query(stmt)
    print(results)
    store.close()
    # No error implies success
    assert True
