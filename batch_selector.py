import math
from sqlalchemy.orm import Session
from sqlalchemy import inspect, exc

class BatchSelector:
    """
    Provides an iterator for fetching SQLAlchemy model instances in batches.

    This class is useful for processing large datasets in chunks, reducing memory
    consumption by loading only a subset of data at a time. It orders results
    by the model's primary key.

    Args:
        session (sqlalchemy.orm.Session): The SQLAlchemy Session object to use for querying.
        model (Type[Base]): The SQLAlchemy model class to be queried (e.g., User).
        batch_size (int): The number of items to fetch in each batch. Must be >= 1.

    Yields:
        list: A list of `model` instances, representing one batch of results.
              The list will be empty if no more items are found.

    Raises:
        ValueError: If `batch_size` is less than 1.
        ValueError: If the `model` cannot be inspected by SQLAlchemy (e.g., not a valid model).
        ValueError: If the `model` does not have a primary key.
        ValueError: If the `model` has a composite primary key (more than one primary key column).
                    This selector currently supports only single-column primary keys.

    Example:
        ```python
        # Assuming User is a SQLAlchemy model and session is an active Session
        # from batch_selector import BatchSelector
        #
        # selector = BatchSelector(session, User, batch_size=100)
        # for user_batch in selector:
        #     for user in user_batch:
        #         print(user.name)
        #     # Process the batch
        ```
    """
    def __init__(self, session: Session, model, batch_size: int):
        if batch_size < 1:
            raise ValueError("batch_size must be greater than or equal to 1")

        self.session = session
        self.model = model
        self.batch_size = batch_size

        try:
            primary_key_columns = inspect(model).primary_key
        except exc.NoInspectionAvailable:
            raise ValueError(f"Could not inspect model {model}. Is it a valid SQLAlchemy model?")

        if not primary_key_columns:
            raise ValueError(f"Model {model} does not have a primary key.")
        if len(primary_key_columns) > 1:
            raise ValueError(f"Model {model} has a composite primary key. This BatchSelector currently supports single-column primary keys only.")
        self.primary_key_column = primary_key_columns[0]

    def __iter__(self):
        offset = 0
        while True:
            batch = self.session.query(self.model).order_by(self.primary_key_column).limit(self.batch_size).offset(offset).all()
            if not batch:
                break
            yield batch
            offset += self.batch_size

if __name__ == '__main__':
    # This is a conceptual example.
    # For this to run, you would need to:
    # 1. Set up a SQLAlchemy engine and session.
    # 2. Define a SQLAlchemy model (e.g., User).
    # 3. Populate the database with some data for that model.

    from sqlalchemy import create_engine, Column, Integer, String
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base

    # Define a simple model for demonstration
    Base = declarative_base()
    class User(Base):
        __tablename__ = 'users'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        def __repr__(self):
            return f"<User(id={self.id}, name='{self.name}')>"

    # Setup in-memory SQLite database for example
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)

    # Create a session
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()

    # Add some dummy data
    users_data = [{'name': f'User {i}'} for i in range(1, 23)]
    for user_data in users_data:
        user = User(**user_data)
        session.add(user)
    session.commit()

    print(f"Total users in DB: {session.query(User).count()}")

    # Example Usage
    batch_size = 5
    selector = BatchSelector(session, User, batch_size)

    batch_number = 1
    for batch in selector:
        print(f"Processing batch {batch_number}:")
        for item in batch:
            print(f"  {item}")
        batch_number += 1
        if batch_number > 5: # Safety break for example, in case of very large datasets
            print("Stopping early for example brevity...")
            break


    session.close()
    print("All batches processed (or example stopped early).")
