import random
import string
from datetime import datetime, timedelta
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import cct_prepare

class RandomJsonGenerator:
    def generate_random_string(self, length=8):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def generate_random_date(self):
        start_date = datetime(year=2020, month=1, day=1)
        end_date = datetime.now()
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start_date + timedelta(days=random_number_of_days)
        return random_date.isoformat()

    def generate_random_json(self):
        return {
            "name": self.generate_random_string(),
            "tradeGroup": f"Group{random.randint(1, 100)}",
            "region": random.randint(1, 5),
            "window": str(random.randint(1, 100)),
            "value": random.randint(1, 500000),
            "blockEnabled": random.randint(1000, 20000),
            "utilizationPct": random.random(),
            "tallyState": random.choice(["NORMAL", "ABNORMAL"]),
            "authorizedBy": self.generate_random_string(5),
            "authorizedDate": self.generate_random_date(),
            "inForce": f"InForce{{type={random.choice(['permanent', 'temporary'])}}}",
            "mt": "message_rates_enrichedcurrent",
            "uuid": self.generate_random_string(12)
        }

    def create_index(self, r):
        schema = (
            NumericField("$.value", as_name="value", sortable=True),
            TagField("$.name", as_name="name", sortable=True),
            NumericField("$.region", as_name="region", sortable=True),
            NumericField("$.blockEnabled", as_name="blockEnabled", sortable=True),
            NumericField("$.utilizationPct", as_name="utilizationPct", sortable=True),
            TagField("$.tallyState", as_name="tallyState", sortable=True),
            TagField("$.authorizedBy", as_name="authorizedBy", sortable=True),
            TagField("$.authorizedDate", as_name="authorizedDate", sortable=True),
            TagField("$.inForce", as_name="inForce", sortable=True),
            TagField("$.mt", as_name="mt", sortable=True),
            TagField("$.uuid", as_name="uuid", sortable=True)
        )
        r.ft(cct_prepare.TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON))

