import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, Float, TEXT
from sqlalchemy.types import Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import MEDIUMTEXT, LONGTEXT


def adapt_column_type(column):
    new_type = column.type
    if isinstance(new_type, (MEDIUMTEXT, LONGTEXT)):
        new_type = Text()  # Replace MySQL-specific type with portable type

    return sqlalchemy.Column(
        column.name,
        new_type,
        *column.constraints,
        primary_key=column.primary_key,
        nullable=column.nullable,
        default=column.default,
        server_default=column.server_default,
        autoincrement=column.autoincrement,
        index=column.index,
        unique=column.unique
    )


def migrate_database(source_url: str, dest_url: str, tables_to_copy: list[str] = None):
    """
    Generic migration function from any SQLAlchemy-supported source DB to destination DB.
    
    Parameters:
        source_url (str): SQLAlchemy database URL of the source.
        dest_url (str): SQLAlchemy database URL of the destination.
        tables_to_copy (list[str], optional): List of table names to migrate. If None, migrate all.
    """

    if source_url == "":
        print("A source url is needed")
        return None
    else:
        print("source url:", source_url)
    
    if dest_url == "":
        print("A destination url is needed")
        return None
    else:
        print("dest url:", dest_url)


    # --- Setup Engines and Sessions ---
    source_engine = sqlalchemy.create_engine(source_url)
    dest_engine = sqlalchemy.create_engine(dest_url)

    SourceSession = sessionmaker(bind=source_engine)
    source_session = SourceSession()

    DestSession = sessionmaker(bind=dest_engine)
    dest_session = DestSession()

    # --- Reflect Source Metadata ---
    source_metadata = sqlalchemy.MetaData()
    source_metadata.reflect(bind=source_engine, only=tables_to_copy)

    # --- Create Tables in Destination ---
    dest_metadata = sqlalchemy.MetaData()
    for table_name, table in source_metadata.tables.items():
        print(f"Creating table '{table_name}' in destination (if not exists)...")
        new_table = Table(table_name, 
                          dest_metadata,
                          *[adapt_column_type(c) for c in table.columns],
                          extend_existing=True)
        new_table.create(bind=dest_engine, checkfirst=True)

    # --- Transfer Data ---
    for table_name, table in source_metadata.tables.items():
        print(f"Transferring data from table '{table_name}'...")
        rows = source_session.execute(table.select()).fetchall()
        if rows:
            dest_table = Table(table_name, dest_metadata, autoload_with=dest_engine)
            dest_session.execute(dest_table.insert(), [dict(row._mapping) for row in rows])
            dest_session.commit()

    # --- Cleanup ---
    print("Data migration complete.")
    source_session.close()
    dest_session.close()
