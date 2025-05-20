import logging
import random

import ydb


def create_table(session):
    session.execute_scheme(
        """
        CREATE TABLE `human` (
            id Uint64,
            name Text,
            age Uint32,
            iq Bool,
            PRIMARY KEY (id)
        );
        """
    )


def insert_data(session, new_human):
    query = f"""
        INSERT INTO human (id, name, iq) VALUES
        ({new_human[0]}, "{new_human[1]}", {new_human[2]});
    """
    logging.debug(query)
    session.transaction().execute(query, commit_tx=True)


def read_data(session):
    query = "SELECT * FROM human;"
    logging.debug(query)
    result_sets = session.transaction().execute(query, commit_tx=True)
    for row in result_sets[0].rows:
        print(f"id: {row.id}, name: {row.name}, age: {row.age}, iq: {row.iq}")


def get_len_human(session):
    query = "SELECT COUNT(*) FROM human;"
    result = session.transaction().execute(query)
    return result[0].rows[0]["column0"]


logging.basicConfig(level=logging.DEBUG)

endpoint = "grpc://localhost:2136"
database = "/Root/test"

driver = ydb.Driver(
    endpoint=endpoint,
    database=database
)

driver.wait(fail_fast=True, timeout=5)

names = [
    "Василий",
    "Мария",
    "Андрей",
    "Александр"
    "Анастасия",
    "Екатерина"
]

session = driver.table_client.session().create()
create_table(session)

n = get_len_human(session)
new_human_data = [n + 1, random.choice(names), random.random() > 0.5]
insert_data(session, new_human_data)
read_data(session)

driver.stop()
