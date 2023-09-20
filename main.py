import sys
import json
from typing import List
from datetime import datetime
from pydantic import BaseModel
from collections import defaultdict

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, JSON, exc, Table

class User(BaseModel):
    __table__ = "users"

    id: int
    name: str
    city: str

class Product(BaseModel):
    __table__ = "products"

    id: int
    name: str
    price: float

class Order(BaseModel):
    __table__ = "orders"

    id: int
    user_id: int
    product_ids: List[int]
    created_at: datetime

class OrdersService:

    def __init__(self, database_url: str):
        self.engine = create_engine(database_url)
        self.metadata = MetaData() 
        self.session = self.engine.connect()

        self.orders_table = Table(
            "orders",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("created_at", DateTime),
            Column("user_id", Integer),
            Column("product_ids", JSON),
        )

        self.users_table = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(255)),
            Column("city", String(255)),
        )

        self.products_table = Table(
            "products",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(255)),
            Column("price", Integer),
        )

        self.metadata.create_all(self.engine)
    
    def insert(self, model_class: BaseModel, model: BaseModel):
        """Inserts a model object into the database.

        Args:
            model_class: The model class of the object to insert.
            model: The object to insert.
        """

        table_name = model_class.__table__
        data = model.__dict__

        try:
            table = Table(table_name, self.metadata, autoload_with=self.session)
        except exc.NoSuchTableError:
            sys.stderr.write(f'Table "{table_name}" does not exist\n')

        # Check if the object already exists in the DB
        if self.session.execute(table.select().where(table.c.id == model.id)) is None:
            stmt = table.insert().values(**data)

            try:
                self.session.execute(stmt)
                self.session.commit()
            except exc.SQLAlchemyError as e:
                self.session.rollback()
                sys.stderr.write(str(e) + '\n')

    def load_orders(self, file_path: str):
        """From the file load the data into the database

        Args:
            file_path: The source data file path.
        """
        with open(file_path, "r") as f:
            num_lines = 0
            for line in f:
                if num_lines % 100 == 0:
                    sys.stdout.write(f'Processing line number {num_lines}\n')
                order = json.loads(line)

                for property in ['id', 'created', 'products', 'user']:
                    if property not in list(order.keys()):
                        sys.stderr.write(f'Order is missing the "{property}" property.\n')
                        continue

                order_id = order["id"]

                # Insert the User into the database
                user = order["user"]
                for property in ["id", "name", "city"]:
                    if property not in user.keys():
                        sys.stderr.write(f'User in order with id: "{order_id}" is missing the "{property}" property.\n')
                        continue

                self.insert(User,
                    User(
                        id=user["id"],
                        name=user["name"],
                        city=user["city"],
                    )
                )

                # Insert the Products into the database
                products = order["products"]
                product_ids = []
                for product in products:
                    for attr in ["id", "name", "price"]:
                        if attr not in product.keys():
                            sys.stderr.write(f'Product in order with id: {order_id} is missing the "{attr}" attribute.\n')
                            continue

                    product_ids.append(product["id"])
                    self.insert(Product,
                        Product(
                            id=product["id"],
                            name=product["name"],
                            price=product["price"],
                        )
                    )

                # Insert the Order into the database
                self.insert(Order,
                    Order(
                        id=order_id,
                        created_at=order["created"],
                        user_id=user["id"],
                        product_ids=product_ids,
                    )
                )
                num_lines += 1
            sys.stdout.write(f'Loaded {num_lines} orders to DB.\n')

    def get_orders_in_period(self, start_date: datetime, end_date: datetime) -> List[Order]:
        """Returns the list of orders within the specified time period

        Args:
            start_date: The begining of the desired period.
            end_date: The end of the desired period.
        """

        orders_query = self.orders_table.select().where(
            self.orders_table.c.created_at.between(start_date, end_date)
        )

        return [order._asdict() for order in self.session.execute(orders_query).fetchall()]

    def get_top_buyers(self, number_of_buyers: int) -> List[User]:
        """Returns a list of the most frequent users taking place the order

        Args:
            number_of_buyers: The size of the response list.
        """

        # users_query = (
        #     self.orders_table
        #     .join(self.users_table, self.orders_table.c.user_id == self.users_table.c.id)
        #     .group_by(self.users_table.c.id)
        #     .select(self.users_table.c.id, self.users_table.c.name, self.users_table.c.city, self.orders_table.c.user_id.count().alias("purchase_count"))
        #     .order_by(self.orders_table.c.quantity.desc())
        #     .limit(number_of_buyers)
        # )

        # users = self.session.execute(users_query).fetchall()

        # return users

        user_purchase_counts = defaultdict(int)
        query = self.orders_table.select(self.orders_table.columns.user_id, self.orders_table.c.product_ids).group_by(self.orders_table.c.user_id).count()
        for user_id, purchase_count in self.session.execute(query).fetchall():
            user_purchase_counts[user_id] = purchase_count

        top_users = [{"user_id": user_id, "purchase_count": purchase_count} for user_id, purchase_count in sorted(user_purchase_counts.items(), key=lambda x: x[1], reverse=True)[:number_of_buyers]]
        self.session.close()

        return top_users

if __name__ == '__main__':
    db_url = 'postgresql://postgres:password@localhost:5432/meiro'
    service = OrdersService(db_url)

    # data_file = 'data.ndjson'
    # service.load_orders(data_file)

    # start_time = '2018-11-04 22:00:00'
    # end_time = '2018-11-04 23:00:00'
    # orders_in_time_range = service.get_orders_in_period(start_time, end_time)
    # sys.stdout.write("\nOrders in time range:\n")
    # for order in orders_in_time_range:
    #     sys.stdout.write(f'{order}\n')

    num_top_users = 5
    top_users = service.get_top_buyers(num_top_users)
    sys.stdout.write("\nTop users by purchase:\n")
    for user in top_users:
        sys.stdout.write(f"User ID: {user['user_id']}, Purchase Count: {user['purchase_count']}\n")
