from faker.providers import BaseProvider
import random
import time
import datetime


class UserBehaviorProvider(BaseProvider):
    def user_id(self):
        user_ids = [x for x in range(1024)]
        # user_ids = [3023,1135,3817,2344,1309,506,3253,3203,2856,2592,2390]
        return random.choice(user_ids)

    def item_id(self):
        validIds = [v for v in range(20, 1024)]
        return random.choice(validIds)

    def behavior(self):
        behaviorNames = ["view", "cart", "buy", "clear", "abandon"]
        return random.choice(behaviorNames)

    def group_name(self):
        groupNames = ["A", "B"]
        return random.choice(groupNames)

    def view_id(self):
        viewIds = [v for v in range(111, 2222, 111)]
        return random.choice(viewIds)

    def produce_msg(self, user_ids = None):
        user_id = self.user_id()
        if (user_ids is not None):
          user_id = random.choice(user_ids)
      
        ts = time.time() - random.randint(-5, 5)
        b = self.behavior()
        view_id = None
        if b == "view":
            view_id = self.view_id()
        message = {
            "user_id": user_id,
            "item_id": self.item_id(),
            "behavior": b,
            "view_id": view_id,
            "group_name": self.group_name(),
            "occurred_at": datetime.datetime.fromtimestamp(ts).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }
        key = {"user": "all_users"}
        return message, key
