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
import json
from typing import Any
from sqlalchemy.types import JSON

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

class JSONBase(DeclarativeBase):
    type_annotation_map = {
        dict[str, Any]: JSON
    }

class JSONItem(JSONBase):
    __tablename__ = "json_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    item: Mapped[dict[str, Any]]

class YosemiteBase(DeclarativeBase):
    pass

class Yosemite(YosemiteBase):
    __tablename__ = "yosemite"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    files: Mapped[List["YosemiteFile"]] = relationship(
        back_populates="yosemite", cascade="all, delete-orphan"
    )
    wind_speed: Mapped[float]
    wdir: Mapped[int]
    smois: Mapped[float]
    fuels: Mapped[str]
    ignition: Mapped[str]
    inside_burned_area: Mapped[int]
    outside_burned_area: Mapped[int]
    
    def __repr__(self) -> str:
        return f"Yosemite(id={self.id!r})"

    
class File(Base):
    __tablename__ = "file"

    id: Mapped[int] = mapped_column(primary_key=True)
    wildfire_id: Mapped[int] = mapped_column(ForeignKey("wildfire.id"))
    path: Mapped[str]
    wildfire: Mapped["Wildfire"] = relationship(back_populates="files")
    def __repr__(self) -> str:
        return f"File(id={self.id!r}, artifact_id={self.wildfire_id!r}, path={self.path!r})"

class YosemiteFile(YosemiteBase):
    __tablename__ = "file"

    id: Mapped[int] = mapped_column(primary_key=True)
    yosemite_id: Mapped[int] = mapped_column(ForeignKey("yosemite.id"))
    path: Mapped[str]
    yosemite: Mapped["Yosemite"] = relationship(back_populates="files")
    def __repr__(self) -> str:
        return f"File(id={self.id!r}, artifact_id={self.yosemite_id!r}, path={self.path!r})"

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

def test_wildfiredata_artifact_put():
    engine_path = "sqlite:///wildfire.db"
    store = SqlAlchemy(engine_path, Base)
    artifacts = []
    wildfire_row = Wildfire(
        wind_speed=9,
        wdir=255,
        smois=0.5,
        fuels='ST5_FF_DUET',
        ignition='ST5_ignite_strip',
        safe_unsafe_ignition_pattern='safe',
        safe_unsafe_fire_behavior='safe',
        does_fire_meet_objectives='Yes',
        rationale_if_unsafe='',
        burned_area=61502,
        files=[File(path='https://wifire-data.sdsc.edu/data//burnpro3d/d/fa/20/run_fa20ed73-8a0b-40e3-bd3f-bca2ff76e3d0/png/run_fa20ed73-8a0b-40e3-bd3f-bca2ff76e3d0_fuels-dens_2100_000.png')]
        )
    artifacts.append(wildfire_row)
    store.put_artifacts(artifacts)
    store.close()
    
    # No error implies success
    assert True

#Data from: https://microsoftedge.github.io/Demos/json-dummy-data/64KB.json
def test_jsondata_artifact_put():
    engine_path = "sqlite:///jsondata.db"
    store = SqlAlchemy(engine_path, JSONBase)
    artifacts = []
    jsonpath = '/'.join([get_git_root('.'), 'dsi/data/64KB.json'])
    try:
        j = open(jsonpath)
        data = json.load(j)
    except IOError as i:
        print(i)
        return
    except ValueError as v:
        print(v)
        return

    artifacts = []
    for d in data:
        print(d)
        json_row = JSONItem(
            item=d
        )
        artifacts.append(json_row)

    store.put_artifacts(artifacts)
    store.close()
    
    # No error implies success
    assert True

def test_yosemite_data_csv_artifact():
    csvpath = '/'.join([get_git_root('.'), 'examples/test/yosemite5.csv'])
    engine_path = "sqlite:///yosemite.db"
    store = SqlAlchemy(engine_path, YosemiteBase)
    print(csvpath)
    with open(csvpath) as csv_file:
        print(csvpath)
        csv_reader = csv.reader(csv_file, delimiter=',')
        header = next(csv_reader)
        artifacts = []
        for row in csv_reader:
            row_zipped = zip(header, row)
            row_dict = dict(row_zipped)
            yosemite_row = Yosemite(
                wind_speed=row_dict['wind_speed'],
                wdir=row_dict['wdir'],
                smois=row_dict['smois'],
                fuels=row_dict['fuels'],
                ignition=row_dict['ignition'],
                inside_burned_area=row_dict['inside_burned_area'],
                outside_burned_area=row_dict['outside_burned_area'],
                files=[YosemiteFile(path=row_dict['FILE'])]
            )
            
            artifacts.append(yosemite_row)
            
    store.put_artifacts(artifacts)
    store.close()
    
    # No error implies success
    assert True

