import random
import time
from pymongo import MongoClient
from faker.providers import BaseProvider

def user_ids_factory():
  range_user_ids = [x for x in range(1024)]
  # range_user_ids = [3023,1135,3817,2344,1309,506,3253,3203,2856,2592,2390]
  
  def range_id():
    chosen_user_id = random.choice(range_user_ids)
    range_user_ids.remove(chosen_user_id)
    return chosen_user_id

  return range_id

get_random_user_id = user_ids_factory()

def get_user_ids(hostname='localhost', port='27017', user='root', password='passwd', database='admin'):
  client = MongoClient(f"mongodb://{user}:{password}@{hostname}:{port}/{database}")
  db = client['local']
  collection = db['users_profile']
  user_ids = []
  for user in collection.find():
    user_ids.append(user['user_id'])
    
  print(f"Retrieved {len(user_ids)} user ids")
  return user_ids



# TODO fill users with some ids from orphan events in event-behaviour collection... just to see 
class UserProvider(BaseProvider):
    def user_id(self):
        chosen_user_id = get_random_user_id()
        return chosen_user_id

    def produce_msg(self, fake_instance):
        ts = time.time() - random.randint(-5, 5)
        user_profile = fake_instance.simple_profile()
        print(user_profile)
        ssn = fake_instance.ssn()
        name = fake_instance.name()
        message = {
            "user_id": self.user_id(),
            "username": user_profile['username'],
            "email": user_profile['mail'],
            "name": user_profile['name'],
            "phone": fake_instance.phone_number(),
            "address": user_profile['address'],
            "ssn": ssn,
            "created_at": int(ts * 1000),
            "updated_at": int(ts * 1000),
        }
        key = {"user": name[0]}
        return message, key
