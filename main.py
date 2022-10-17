import os

from dotenv import load_dotenv

from pymongo import MongoClient

env = os.path.join(os.path.abspath(os.curdir), '.env')
print(env)