import sqlalchemy
import json
import os
from sqlalchemy.orm import sessionmaker

from models import create_tables, Publisher, Book, Shop, Stock, Sale

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
DBNAME = os.getenv("DBNAME")

# USERNAME = "postgres"
# PASSWORD = "postgres"
# DBNAME = "books"

DSN = f'postgresql://{USERNAME}:{PASSWORD}@localhost:5432/{DBNAME}'

engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('tests_data.json', 'r', encoding='utf-8') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))

publisher_name = input('Введите наименование издателя: ')

for row in (session.query(Publisher, Book, Shop, Stock).select_from(Publisher)
        .join(Book)
        .join(Stock)
        .join(Shop)
        .filter(Publisher.name == publisher_name)
        ).all():
    print(*row, sep='\t | ')

session.commit()
session.close()
