import json
import sys
from datetime import datetime
from typing import List, Dict, Union
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Table, Float, ForeignKey, exc, func, text
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.orm import sessionmaker, relationship
from collections import defaultdict

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

Base = declarative_base()

# Table for N:M relationship between order and product
order_product = Table(
    'order_product',
    Base.metadata,
    Column('order_id', Integer, ForeignKey('orders.id'), primary_key=True),
    Column('product_id', Integer, ForeignKey('products.id'), primary_key=True),
    Column('quantity', Integer, nullable=False)
)

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    city = Column(String)
    orders = relationship('Order', back_populates='user')

class Product(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    price = Column(Float)
    orders = relationship('Order', secondary=order_product, back_populates='products')

class Order(Base):
    __tablename__ = 'orders'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    created = Column(DateTime)
    user = relationship('User', back_populates='orders')
    products = relationship('Product', secondary=order_product, back_populates='orders')

class OrdersService:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def add_to_session(self, session: Session, object: Union[User, Product, Order], check_if_exists: bool = True):
        object_exists = False
        if check_if_exists:
            object_exists = session.query(type(object)).filter(type(object).id == object.id).scalar()

        if object_exists:
            session.merge(object)
        elif not object_exists:
            session.add(object)

    def try_commit(self, session: Session):
        try:
            session.commit()
        except exc.SQLAlchemyError as e:
            session.rollback()
            sys.stderr.write(str(e) + '\n')

    def deduplicate_list_of_order_product_items(self, list_items):
        seen = set()
        deduplicated_list = []
        for item in list_items:
            # example item -> {'order_id': 3, 'product_id': 6, 'quantity': 1}
            id = (item['order_id'], item['product_id'])
            if id not in seen:
                seen.add(id)
                deduplicated_list.append(item)
        return deduplicated_list

    def load_data_from_file(self, data_file: str):
        """Loads the data from given file to the database with respect to object relationships."""

        session = self.Session()

        num_lines = 0
        with open(data_file, 'r') as file:
            for line in file:
                sys.stdout.write(f'\nProcessing order: {num_lines}')
                data = json.loads(line)

                # Check the order properties presence before loading
                for property in ['id', 'created', 'products', 'user']:
                    if property not in list(data.keys()):
                        sys.stderr.write(f'Order is missing the "{property}" property.\n')
                        continue

                order_id = data['id']
                user_data = data['user']
                
                # Check the user properties presence before loading
                for property in ['id', 'name', 'city']:
                    if property not in user_data.keys():
                        sys.stderr.write(f'User in order with id: "{order_id}" is missing the "{property}" property.\n')
                        continue

                # Create a User object
                user = User(id=user_data['id'], name=user_data['name'], city=user_data['city'])
                self.add_to_session(session, user)

                # Add the Order object to the session and commit
                order = Order(id=order_id, user_id=user.id, created=datetime.fromtimestamp(data['created']))
                self.add_to_session(session, order)
                self.try_commit(session)

                product_ids = []
                quantity_map = defaultdict(int)
                for product_data in data['products']:
                    # Check the product properties presence before loading
                    for attr in ['id', 'name', 'price']:
                        if attr not in product_data.keys():
                            sys.stderr.write(f'Product in order with id: "{order_id}" is missing the "{attr}" attribute.\n')
                            continue
                    product_id = product_data['id']
                    quantity_map[product_id] += 1
                    product_ids.append(product_id)

                # Add the Product objects to the session and commit them
                for product_id in product_ids:
                    product = Product(id=product_id, name=product_data['name'], price=product_data['price'])
                    self.add_to_session(session, product)
                    self.try_commit(session)

                # Now, you can insert data into the order_product table if it doesn't already exist
                order_product_data = []
                for product_id in product_ids:
                    if not session.query(order_product).filter(order_product.c.order_id == order.id, order_product.c.product_id == product_id).count():
                        order_product_data.append({'order_id': order.id, 'product_id': product_id, 'quantity': quantity_map[product_id]})

                # There are duplicates in order_product_data which are caused by multiple occurrences of same product in one order.
                order_product_data = self.deduplicate_list_of_order_product_items(order_product_data)

                if order_product_data:
                    session.execute(order_product.insert().values(order_product_data))

                # Commit the changes again to insert order_product records if any
                self.try_commit(session)

                num_lines += 1

        sys.stdout.write(f'Total lines processed: {num_lines}\n')

    def get_product_ids_for_order(self, session: Session, order: Order) -> List[int]:
        """Helper method to get list of product ids for a given order."""

        order_products = session.query(order_product).filter(order_product.c.order_id == order.id).all()
        product_ids = []
        # Append the product_id to the order number of times according to the order_product quantity column
        for _, product_id, quantity in order_products:
            product_ids += [product_id for i in range(quantity)]
        return product_ids

    def get_orders_in_time_range(self, start_time: str, end_time: str) -> List[Dict]:
        """Method filter out orders which are out of a given time range and retuns only those in that time range."""

        session = self.Session()

        orders = session.query(Order).filter(Order.created.between(start_time, end_time)).order_by(Order.created).all()
        result = []
        for order in orders:
            result.append({
                "id":order.id,
                "user_id": order.user_id,
                "product_ids": self.get_product_ids_for_order(session, order),
                "created": order.created.strftime(DATE_TIME_FORMAT)
                }
            )
        session.close()
        return result

    def get_top_users_by_product_purchase_count(self, num_users: int) -> List[Dict]:
        """Method returns given number of user who has the most products ordered."""

        session = self.Session()

        # Get the products count for each order
        subquery_order = (
            session.query(Order.id.label('order_id'), func.sum(order_product.c.quantity).label('product_quantity'))
            .join(order_product, Order.id == order_product.c.order_id)
            .group_by(Order.id)
            .subquery()
        )
        # Aggregate by users which gives the final product count for each user
        subquery_aggregated = (
            session.query(Order.user_id, func.sum(subquery_order.c.product_quantity).label('purchase_count'))
            .join(subquery_order, subquery_order.c.order_id == Order.id)
            .group_by(Order.user_id)
            .order_by(text('purchase_count DESC'))
            .limit(num_users)
            .subquery()
        )
        # Join with users for user details
        query = (
            session.query(User.id, User.name, User.city, subquery_aggregated.c.purchase_count)
            .join(subquery_aggregated, subquery_aggregated.c.user_id == User.id)
            .order_by(subquery_aggregated.c.purchase_count.desc())
        )

        top_users = []
        for user_id, user_name, user_city, purchase_count in query:
            top_users.append({
                    'user_id': user_id,
                    'user_name': user_name,
                    'user_city': user_city,
                    'purchase_count': purchase_count
                }
            )

        session.close()

        return top_users

if __name__ == '__main__':
    db_url = 'postgresql://postgres:password@localhost:5432/meiro'
    service = OrdersService(db_url)

    data_file = 'data-example.ndjson'
    service.load_data_from_file(data_file)

    start_time = '2018-10-20 17:00:00'
    end_time = '2018-10-25 22:00:00'
    orders_in_time_range = service.get_orders_in_time_range(start_time, end_time)
    sys.stdout.write('Orders in time range:\n')
    for order in orders_in_time_range:
        sys.stdout.write(f'Order id: {order["id"]}, created: {order["created"]}, product_ids: {order["product_ids"]}, user id: {order["user_id"]}\n')

    num_top_users = 5
    top_users = service.get_top_users_by_product_purchase_count(num_top_users)
    sys.stdout.write('\nTop users by purchase:\n')
    for user in top_users:
        sys.stdout.write(f'User id: {user["user_id"]}, name: {user["user_name"]}, city: {user["user_city"]}, purchase products count: {user["purchase_count"]}\n')
