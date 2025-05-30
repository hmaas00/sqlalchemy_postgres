import unittest
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine, Column, Integer, String, inspect, exc
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from batch_selector import BatchSelector

# Define a simple model for demonstration
Base = declarative_base()

class User(Base):
    __tablename__ = 'test_users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)

    def __repr__(self):
        return f"<User(id={self.id}, name='{self.name}')>"

# This model will be defined WITH a PK, but we will mock inspect() for it
# in the specific test case to simulate it having no PK.
class ModelForNoPKTest(Base):
    __tablename__ = 'test_model_for_no_pk'
    id = Column(Integer, primary_key=True)
    name = Column(String)

class CompositePKModel(Base):
    __tablename__ = 'test_composite_pk'
    id1 = Column(Integer, primary_key=True)
    id2 = Column(Integer, primary_key=True)
    name = Column(String)


class TestSQLAlchemyBatchSelector(unittest.TestCase):
    engine = None
    SessionLocal = None

    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(cls.engine)
        cls.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=cls.engine)

    @classmethod
    def tearDownClass(cls):
        Base.metadata.drop_all(cls.engine)

    def setUp(self):
        self.session = self.SessionLocal()
        self._populate_data(22) # Add 22 users for general tests

    def tearDown(self):
        # Clean up data from all tables after each test
        for table in reversed(Base.metadata.sorted_tables):
            self.session.execute(table.delete())
        self.session.commit()
        self.session.close()

    def _populate_data(self, num_users):
        users_data = [{'name': f'User {i}'} for i in range(1, num_users + 1)]
        for user_data in users_data:
            user = User(**user_data)
            self.session.add(user)
        self.session.commit()

    def test_invalid_batch_size(self):
        with self.assertRaisesRegex(ValueError, "batch_size must be greater than or equal to 1"):
            BatchSelector(self.session, User, 0)
        with self.assertRaisesRegex(ValueError, "batch_size must be greater than or equal to 1"):
            BatchSelector(self.session, User, -1)

    @patch('batch_selector.inspect') # Patch inspect in the context of batch_selector module
    def test_model_without_primary_key(self, mock_inspect):
        # Configure the mock inspect to return an empty list for primary_key
        mock_inspector = MagicMock()
        mock_inspector.primary_key = []
        mock_inspect.return_value = mock_inspector

        with self.assertRaisesRegex(ValueError, f"Model {ModelForNoPKTest} does not have a primary key."):
            BatchSelector(self.session, ModelForNoPKTest, 5)
        mock_inspect.assert_called_once_with(ModelForNoPKTest)

    def test_model_with_composite_primary_key(self):
        with self.assertRaisesRegex(ValueError, f"Model {CompositePKModel} has a composite primary key. This BatchSelector currently supports single-column primary keys only."):
            BatchSelector(self.session, CompositePKModel, 5)
    
    def test_model_not_a_sqlalchemy_model(self):
        class NotAModel:
            pass
        with self.assertRaisesRegex(ValueError, f"Could not inspect model {NotAModel}. Is it a valid SQLAlchemy model?"):
            BatchSelector(self.session, NotAModel, 5)


    def test_empty_table(self):
        # Clear existing data
        self.session.query(User).delete()
        self.session.commit()

        selector = BatchSelector(self.session, User, 5)
        batches = list(selector) # Convert iterator to list
        self.assertEqual(len(batches), 0)

    def test_perfect_division(self):
        # Clear and repopulate for specific count
        self.session.query(User).delete()
        self.session.commit()
        self._populate_data(10) # 10 users

        selector = BatchSelector(self.session, User, 2)
        batches = list(selector)
        self.assertEqual(len(batches), 5)
        for i, batch in enumerate(batches):
            self.assertEqual(len(batch), 2)
            self.assertEqual(batch[0].id, i * 2 + 1)
            self.assertEqual(batch[1].id, i * 2 + 2)

    def test_imperfect_division(self):
        # Uses the default 22 users from setUp
        selector = BatchSelector(self.session, User, 5)
        batches = list(selector)
        self.assertEqual(len(batches), 5) # 22 items, batch size 5 -> 4 full batches, 1 partial

        self.assertEqual(len(batches[0]), 5)
        self.assertEqual(len(batches[1]), 5)
        self.assertEqual(len(batches[2]), 5)
        self.assertEqual(len(batches[3]), 5)
        self.assertEqual(len(batches[4]), 2) # Last batch

        all_retrieved_users = []
        for batch in batches:
            for user in batch:
                all_retrieved_users.append(user.id)
        
        self.assertEqual(len(all_retrieved_users), 22)
        self.assertEqual(sorted(all_retrieved_users), list(range(1, 23)))


    def test_batch_size_larger_than_items(self):
        # Uses the default 22 users from setUp
        selector = BatchSelector(self.session, User, 30) # Batch size larger than 22 items
        batches = list(selector)
        self.assertEqual(len(batches), 1)
        self.assertEqual(len(batches[0]), 22)

        all_retrieved_users = [user.id for user in batches[0]]
        self.assertEqual(sorted(all_retrieved_users), list(range(1, 23)))
        
    def test_iteration_yields_correct_data_and_order(self):
        # Clear and repopulate for specific count and known data
        self.session.query(User).delete()
        self.session.commit()
        self._populate_data(7)

        selector = BatchSelector(self.session, User, 3)
        expected_batches = [
            [1, 2, 3],
            [4, 5, 6],
            [7]
        ]
        
        batch_idx = 0
        for batch in selector:
            self.assertTrue(batch_idx < len(expected_batches), "More batches yielded than expected")
            expected_ids = expected_batches[batch_idx]
            self.assertEqual(len(batch), len(expected_ids), f"Batch {batch_idx} has wrong size")
            for item_idx, item in enumerate(batch):
                self.assertIsInstance(item, User)
                self.assertEqual(item.id, expected_ids[item_idx], f"Item id mismatch in batch {batch_idx}")
            batch_idx += 1
        self.assertEqual(batch_idx, len(expected_batches), "Not all expected batches were yielded")


if __name__ == '__main__':
    unittest.main()
