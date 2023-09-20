import json
import sys
from datetime import datetime
from typing import List, Dict, Union
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, ForeignKey, Table, exc, func, select, join
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.orm import sessionmaker, relationship
from collections import defaultdict

Base = declarative_base()

# Table for N:M relationship between order and product
order_product = Table(
    'order_product',
    Base.metadata,
    Column('order_id', Integer, ForeignKey('orders.id'), primary_key=True),
    Column('product_id', Integer, ForeignKey('products.id'), primary_key=True)
)

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    city = Column(String)
    orders = relationship("Order", back_populates="user")

class Product(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    price = Column(Float)
    orders = relationship("Order", secondary=order_product, back_populates="products")

class Order(Base):
    __tablename__ = 'orders'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    created = Column(DateTime)
    user = relationship("User", back_populates="orders")
    products = relationship("Product", secondary=order_product, back_populates="orders")

class OrdersService:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def add_to_session(self, session: Session, object: Union[User, Product, Order], check_if_exists: bool = True) -> Session: 
        object_exists = False
        if check_if_exists:
            object_exists = session.query(type(object)).filter(type(object).id == object.id).scalar()

        if object_exists:
            session.merge(object)
        elif not object_exists:
            session.add(object)
        return session

    def try_commit(self, session: Session):
        try:
            session.commit()
        except exc.SQLAlchemyError as e:
            session.rollback()
            sys.stderr.write(str(e) + '\n')

    def load_data_from_file(self, data_file: str):
        session = self.Session()

        num_lines = 0
        with open(data_file, 'r') as file:
            for line in file:
                print(f'\nProcessing order: {num_lines}')
                data = json.loads(line)
                # Check the order properties presence before loading
                for property in ['id', 'created', 'products', 'user']:
                    if property not in list(data.keys()):
                        sys.stderr.write(f'Order is missing the "{property}" property.\n')
                        continue

                order_id = data["id"]
                user = data["user"]
                # Check the user properties presence before loading
                for property in ["id", "name", "city"]:
                    if property not in user.keys():
                        sys.stderr.write(f'User in order with id: "{order_id}" is missing the "{property}" property.\n')
                        continue

                self.add_to_session(session, User(id=user['id'], name=user['name'], city=user['city']))
                self.try_commit(session)

                products = []
                for product_data in data['products']:
                    # Check the product properties presence before loading
                    for attr in ["id", "name", "price"]:
                        if attr not in product_data.keys():
                            sys.stderr.write(f'Product in order with id: {order_id} is missing the "{attr}" attribute.\n')
                            continue
                    product_id = product_data['id']
                    existing_product = session.query(Product).filter_by(id=product_id).first()
                    if existing_product:
                        products.append(existing_product)
                    else:
                        product = Product(id=product_id, name=product_data['name'], price=product_data['price'])
                        self.add_to_session(session, product, check_if_exists=False)
                        self.try_commit(session)
                        products.append(product)

                order = Order(id=order_id, user_id=user['id'], created=datetime.fromtimestamp(data['created']), products=products)
                self.add_to_session(session, order)
                self.try_commit(session)
                # order.products = products
                # session.merge(order)
                # self.try_commit(session)

                num_lines += 1
        print(f'Total lines processed: {num_lines}')

    def get_orders_in_time_range(self, start_time: str, end_time: str) -> List[Dict]:
        session = self.Session()

        orders = session.query(Order).filter(Order.created.between(start_time, end_time)).all()

        result = [{"id":order.id, "user_id": order.user_id, "product_ids": [product.id for product in order.products], "created": order.created} for order in orders]
        session.close()
        return result

    def get_top_users_by_purchase(self, num_users: int) -> List[Dict]:
        session = self.Session()
        stmt = select([Order.user_id, func.count()]).select_from(join(Order, order_product, isouter=True).join(Product, isouter=True)).group_by(Order.user_id)
        
        user_purchase_counts = defaultdict(int)

        # Use SQLAlchemy ORM to construct the query
        query = (
            session.query(User.id, func.count(Order.id).label('purchase_count'))
            .outerjoin(Order)
            .group_by(User.id)
            .order_by(func.count(Order.id).desc())
            .limit(num_users)
        )

        for user_id, purchase_count in query:
            user_purchase_counts[user_id] = purchase_count

        top_users = [{"user_id": user_id, "purchase_count": purchase_count} for user_id, purchase_count in user_purchase_counts.items()]
        session.close()

        return top_users
        # user_purchase_counts = defaultdict(int)
        # for user_id, purchase_count in session.query(Order.user_id, func.count(Order.products)).group_by(Order.user_id):
        #     user_purchase_counts[user_id] = purchase_count

        # top_users = [{"user_id": user_id, "purchase_count": purchase_count} for user_id, purchase_count in sorted(user_purchase_counts.items(), key=lambda x: x[1], reverse=True)[:num_users]]

        # session.close()

        # return top_users

if __name__ == '__main__':
    db_url = 'postgresql://postgres:password@localhost:5432/meiro'
    service = OrdersService(db_url)

    data_file = 'data-example.ndjson'
    service.load_data_from_file(data_file)

    start_time = '2018-10-25 17:00:00'
    end_time = '2018-10-25 22:00:00'
    orders_in_time_range = service.get_orders_in_time_range(start_time, end_time)
    print("Orders in time range:")
    for order in orders_in_time_range:
        print(order)

    # num_top_users = 5
    # top_users = service.get_top_users_by_purchase(num_top_users)
    # print("\nTop users by purchase:")
    # for user in top_users:
    #     print(f"User ID: {user['user_id']}, Purchase Count: {user['purchase_count']}")
