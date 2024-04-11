import datetime
import unittest

import pymongo
from bson import ObjectId
from pymongo import MongoClient

from settings import mongo_uri


class ConnTest(unittest.TestCase):
    def setUp(self):
        self.c = MongoClient(mongo_uri)
        self.db = self.c.get_database()

    def test_conn(self):
        db = self.c.get_database()
        print(db.list_collection_names())

    def test_db(self):
        self.assertEqual(self.c.get_database().name, "h6")
        self.assertEqual(self.c.h6.name, "h6")

    def test_collection(self):
        collection = self.c.get_database().get_collection("info")
        self.assertIsNotNone(collection)
        self.assertEqual(self.c.h6.info.name, "info")

    def test_post(self):
        post = {
            "author": "Mike",
            "text": "My first blog post!",
            "tags": ["mongodb", "python", "pymongo"],
            "date": datetime.datetime.now(tz=datetime.timezone.utc),
        }
        post_id = self.c.h6.posts.insert_one(post).inserted_id
        self.assertTrue(isinstance(post_id, ObjectId))

    def test_new_collection(self):
        collections = self.c.get_database().list_collection_names()
        self.assertIn("posts", collections)

    def test_find_one(self):
        p = self.db.posts.find_one()
        self.assertIsNotNone(p)

    def test_find_one_q(self):
        p = self.db.posts.find_one({"author": "Mike"})
        self.assertEqual(p["author"], "Mike")
        n = self.db.posts.find_one({"author": "Eliot"})
        self.assertIsNone(n)

    def test_object_id(self):
        post = {
            "author": "Jake",
            "text": "ObjectId demo!",
            "tags": ["mongodb", "python", "pymongo"],
            "date": datetime.datetime.now(tz=datetime.timezone.utc),
        }
        post_id = self.db.posts.insert_one(post).inserted_id
        self.assertIsNotNone(self.db.posts.find_one({"_id": post_id}))
        post_id_str = str(post_id)
        self.assertIsNone(self.db.posts.find_one({"_id": post_id_str}))
        self.assertIsNotNone(self.db.posts.find_one({"_id": ObjectId(post_id_str)}))

    def test_bulk_inserts(self):
        posts = [
            {
                "author": "Mike",
                "text": "Another post!",
                "tags": ["bulk", "insert"],
                "date": datetime.datetime(2009, 11, 12, 11, 14),
            },
            {
                "author": "Eliot",
                "title": "MongoDB is fun",
                "text": "and pretty easy too!",
                "date": datetime.datetime(2009, 11, 10, 10, 45),
            },
        ]
        c = self.db.posts.insert_many(posts).inserted_ids
        self.assertEqual(len(c), 2)

    def test_count(self):
        self.assertEqual(self.db.posts.count_documents({}), 4)

    def test_count_q(self):
        self.assertEqual(self.db.posts.count_documents({"author": "Mike"}), 2)

    def test_range_query(self):
        d = datetime.datetime(2009, 11, 12, 12)
        ps = self.db.posts.find({"date": {"$lt": d}}).sort("author")
        for p in ps:
            print(p)

    def test_indexing(self):
        self.db.profiles.create_index([("user_id", pymongo.ASCENDING)], unique=True)
        self.assertIn("user_id_1", self.db.profiles.index_information())

    def test_unique_index(self):
        user_profiles = [
            {"user_id": 211, "name": "Luke"},
            {"user_id": 212, "name": "Ziltoid"},
        ]
        self.db.profiles.insert_many(user_profiles)
        self.db.profiles.insert_one({"user_id": 213, "name": "Drew"})
    def test_duplicate_insert(self):
        
        try:
            self.db.profiles.insert_one({"user_id": 212, "name": "Tommy"})
        except pymongo.errors.DuplicateKeyError:
            pass
        else:
            self.fail("Should have raised DuplicateKeyError")
