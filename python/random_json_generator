import random
import string
from datetime import datetime, timedelta

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
            "region": f"region{random.randint(1, 5)}",
            "window": str(random.randint(1, 100)),
            "value": random.randint(10000, 500000),
            "blockEnabled": random.randint(1000, 20000),
            "utilizationPct": random.random(),
            "tallyState": random.choice(["NORMAL", "ABNORMAL"]),
            "authorizedBy": self.generate_random_string(5),
            "authorizedDate": self.generate_random_date(),
            "inForce": f"InForce{{type={random.choice(['permanent', 'temporary'])}}}",
            "mt": "message_rates_enrichedcurrent",
            "uuid": self.generate_random_string(12)
        }
