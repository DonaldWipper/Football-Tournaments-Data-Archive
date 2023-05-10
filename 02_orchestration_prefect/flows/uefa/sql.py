import os
from sqlalchemy.engine import create_engine
from sqlalchemy import MetaData, Table, select, and_
from prefect.blocks.system import Secret

secret_block = Secret.load("mysql-secrets")

def get_dict_proxy(result: str) -> dict:
    return [dict(r) for r in result]


class DBConnection:
    engine = None

    url = secret_block.get()

    @classmethod
    def get_engine(cls, new=False):
        """Creates return new Singleton database connection"""
        if new or not cls.engine:
            cls.engine = create_engine(cls.url, echo=True)
        return cls.engine

    def __repr__(self):
        return "DB(host='%s', name='%s')" % (self.host, self.name)

    def __str__(self):
        return "<%s dv named %s>" % (self.host, self.name)

    @classmethod
    def execute(cls, query):
        engine = cls.get_engine()
        with engine.connect() as connection:
            result = connection.execute(query)
            return result

    @classmethod
    def get_dict_from_table(cls, table_name, condition=None):
        meta = MetaData(cls.get_engine())
        meta.reflect(only=[table_name])
        table = Table(table_name, meta)
        where_st = []
        if condition:
            for key, value in condition.items():
                where_st.append((table.c[key] == value))
            select_st = select(table).where(and_(table.c[key] == value))
        else:
            select_st = select(table)

        return get_dict_proxy(cls.execute(select_st))

    @classmethod
    def get_dict_from_query(cls, query):
        return get_dict_proxy(cls.execute(query))
