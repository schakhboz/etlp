import datetime
from sqlalchemy import MetaData, create_engine, Table
import sqlalchemy
import pandas
import configparser
import connectorx as cx


def is_create_table(table_name, source_table, source_engine, target_engine):
    try:
        inspect_source_engine = sqlalchemy.inspect(source_engine)
        inspect_target_engine = sqlalchemy.inspect(target_engine)

        query = """CREATE TABLE {} (""".format(table_name)

        if not inspect_target_engine.has_table(table_name):
            for column in source_table.columns:
                if column.nullable:
                    nullable = 'NULL'
                else:
                    nullable = 'NOT NULL'
                query = query + '{} {} {}, '.format(column.name, column.type, nullable)
            if inspect_source_engine.get_pk_constraint(table_name)['constrained_columns']:
                for constraint in source_table.constraints:
                    query = query + 'CONSTRAINT {}'.format(constraint.name)
            for pk in source_table.primary_key:
                query = query + ' PRIMARY KEY ({}),'.format(pk.name)
            query = query.rstrip(', ') + """)"""

            with target_engine.connect() as con:
                con.execute(query)
            return True
        return True
    except Exception as e:
        print(e)
        return False


def main():
    config_file = configparser.ConfigParser()
    config_file.read('configurations.ini')

    table_name = config_file['TABLE']['name']

    source_engine = create_engine(config_file["DATABASE"]["source_db"])

    source_metadata = MetaData(bind=source_engine)
    source_table = Table(table_name, source_metadata, autoload_with=source_engine)

    target_engine = create_engine(config_file["DATABASE"]["target_db"])

    create_table = is_create_table(table_name=table_name, source_table=source_table, source_engine=source_engine,
                                   target_engine=target_engine)

    if create_table:
        query_last_item_target_table = 'select id from {} order by id desc limit 1'.format(table_name)
        df = pandas.read_sql_query(query_last_item_target_table, target_engine)

        if df.empty:
            item = 0
            query = 'select * from {} order by id asc'.format(table_name)
        else:
            item = int(df.iloc[0][0])
            query = 'select * from {} where id > {} and id < '.format(table_name, item)
        print("Start time: ", datetime.datetime.now())
        try:
            # last_id = cx.read_sql("postgresql://postgres:zLeBedc4tkij@192.168.158.143:3245/dms_db_node3",
            #                       'select id from {} order by id desc limit 1'.format(table_name))
            # iter = 0
            #
            # while iter <= int(last_id.iloc[0][0]):
            #     k = iter + 50000
            #     select_to_query = "".join(("select * from {} where id >= {} and id <".format(table_name, iter),
            #                                "{} order by id asc".format(k)))
            #     dfcx = cx.read_sql("postgres://postgres:zLeBedc4tkij@192.168.158.143:3245/dms_db_node3",
            #                        select_to_query)
            #     iter = k
            #     dfcx.to_sql(con=target_engine, name=table_name, if_exists='append', index=False)
            query_offset = "".join((query, "10"))
            print(query_offset)
            df = cx.read_sql(config_file["DATABASE"]["source_db"], query)
            for chunk in pandas.read_sql_query(query, source_engine, chunksize=50000):
                chunk.to_sql(con=target_engine, name=table_name, if_exists='append', index=False)
        except Exception as e:
            print(e)

        print("Finish time: ", datetime.datetime.now())


if __name__ == '__main__':
    main()
