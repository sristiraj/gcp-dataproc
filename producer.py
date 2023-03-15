from google.cloud import pubsub_v1
import os
import random
import json
import time

from google.cloud.pubsublite.cloudpubsub import PublisherClient
from google.cloud.pubsublite.types import (
        CloudRegion,
        CloudZone,
        MessageMetadata,
        TopicPath,
    )

project_id = 'data-lake-project-379413'
topic = 'datalake-demo'
topic_id = topic
location = 'us-east1-b'
shipment_id = 100000
shipper = ['Airtight Shipping','Federal Amulgated','Greenway Carriers','Delagato''s Express','Xpressway']
origin = ['AZ', 'SC', 'LA', 'MN', 'NJ', 'MX', 'DC', 'OR', 'VA', 'RI', 'KY', 'NH', 'MI', 'NV', 'WI', 'ID', 'NC', 'CA', 'NE', 'CT', 'MT', 'PQ', 'MD', 'DE', 'MO', 'IL', 'ME', 'MB', 'WA', 'AL', 'IN', 'OH', 'TN', 'NM', 'IA', 'PA', 'SD', 'NY', 'ON', 'TX', 'GA', 'MA', 'KS', 'CO', 'FL', 'AR', 'NB', 'OK', 'UT']
destination = ['AZ', 'SC', 'LA', 'MN', 'NJ', 'MX', 'DC', 'OR', 'VA', 'RI', 'KY', 'NH', 'MI', 'NV', 'WI', 'ID', 'NC', 'CA', 'NE', 'CT', 'MT', 'PQ', 'MD', 'DE', 'MO', 'IL', 'ME', 'MB', 'WA', 'AL', 'IN', 'OH', 'TN', 'NM', 'IA', 'PA', 'SD', 'NY', 'ON', 'TX', 'GA', 'MA', 'KS', 'CO', 'FL', 'AR', 'NB', 'OK', 'UT']
customer = ['Accent on Style', 'Nextalis', 'Inotech Ltd.', 'Blue Heron', 'Eliza Software', "Viktoria's", 'HP', 'Value Mart', 'Spirit Soda', 'Koncept', 'Aqua LLC', 'Endova Corp', 'Cathay Air', 'General Food', 'Kyle Industries', 'Anova', 'Calexico', 'Firebright', 'Alpha Channel', 'DC MTA', 'Wayerhouse', 'M-Tell Wireless']

# publisher = pub
# sub_v1.PublisherClient()

topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=project_id,
    topic=topic,  # Set this to something appropriate.
)

subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
    project_id=project_id,
    sub='datalake-sub',  # Set this to something appropriate.
)
regional = False
cloud_region = 'us-east1'
zone_id = "b"

if regional:
    location = CloudRegion(cloud_region)
else:
    location = CloudZone(CloudRegion(cloud_region), zone_id)

print(location)
topic_path = TopicPath(project_id, location, topic_id)

def callback(message):
    print(message.data)
    message.ack()

# with pubsub_v1.SubscriberClient() as subscriber:
#     subscriber.create_subscription(
#         name=subscription_name, topic=topic_name)
#     future = subscriber.subscribe(subscription_name, callback)
with PublisherClient() as publisher_client:
    while True:
        delay_kpi = random.uniform( -10, 10 )
        shipment_id = shipment_id+1
        shipper_val = shipper 
        payload = {"delay_kpi":delay_kpi, "shipment_id": shipment_id, "shipper":random.choice(shipper), "customer": random.choice(customer), "origin_state": random.choice(origin), "destination_state": random.choice(destination), "avg_delay": random.randint(-5,5), "pay_amount": random.randint(1,10000)}
        print(payload)
        # future = publisher_client.publish(topic_name, json.dumps(payload, indent=2).encode('utf-8'), spam='eggs')
        future = publisher_client.publish(topic_path, json.dumps(payload, indent=2).encode('utf-8'), ordering_key="testing")
        print(future.result())
        time.sleep(10)    