from kafka import KafkaConsumer
import json
from s3fs import S3FileSystem
consumer = KafkaConsumer(
        "user",
        bootstrap_servers=['localhost:9092'],
        value_deserializer= json.loads,
        auto_offset_reset = "latest"
    )

# consumer.subscribe("user")

# # while True:
# #     data = next(consumer)
# #     print(data)
# #     print(data.value)

# data = next(consumer)
# for i in data:
#     print(i)

s3 = S3FileSystem()

for count , i in enumerate(consumer):
    with s3.open("s3://user-data-kafka-bhavya/user_data_{}.json".format(count),'w') as file:
        json.dump(i.value , file)
