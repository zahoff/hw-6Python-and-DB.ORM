import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

    def __repr__(self):
        return f'Издатель: {self.name}'


class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String, nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False, index=True)

    publishers = relationship(Publisher, backref="books")

    def __repr__(self):
        return f'Книга: {self.title}'


class Shop(Base):
    __tablename__ = 'shop'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String, unique=True)

    def __repr__(self):
        return f'Магазин: {self.name}'


class Stock(Base):
    __tablename__ = 'stock'

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey('book.id'), index=True)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey('shop.id'), index=True)
    count = sq.Column(sq.Integer, nullable=False)

    books = relationship(Book, backref='stock')
    shops = relationship(Shop, backref='stock')

    def __repr__(self):
        return f'Запас: {self.count}'


class Sale(Base):
    __tablename__ = 'sale'

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable=False)
    date_sale = sq.Column(sq.DateTime, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey('stock.id'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    stock = relationship(Stock, backref='sale')


def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
