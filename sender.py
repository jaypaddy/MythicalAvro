
import sys
import logging
import datetime
import time
import os
import json
import avro.schema
import io, random
from avro.io import DatumWriter
from avro.datafile import DataFileReader, DataFileWriter


from azure.eventhub import EventHubClient, Sender, EventData

logger = logging.getLogger("azure")

# Address can be in either of these formats:
# "amqps://<URL-encoded-SAS-policy>:<URL-encoded-SAS-key>@<namespace>.servicebus.windows.net/eventhub"
# "amqps://<namespace>.servicebus.windows.net/<eventhub>"
# SAS policy and key are not required if they are encoded in the URL

ADDRESS = "amqps://xxxxx.servicebus.windows.net/avro"
USER = "<PolicyName>"
KEY = "yyyyyyy="

SCHEMA = {"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}

try:
    if not ADDRESS:
        raise ValueError("No EventHubs URL supplied.")

    # Create Event Hubs client
    client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
    sender = client.add_sender(partition="0")
    client.run()
    try:
        start_time = time.time()
        for i in range(100):
            print("Sending message: {}".format(i))
            writer = DatumWriter(SCHEMA)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), SCHEMA)
            writer.append({"name": "Alyssa", "favorite_number": 256})
            writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
            writer.close()
            writer.write({"name": "123", "favorite_number": 10}, encoder)
            raw_bytes = bytes_writer.getvalue()
            sender.send(EventData(raw_bytes))
    except:
        raise
    finally:
        end_time = time.time()
        client.stop()
        run_time = end_time - start_time
        logger.info("Runtime: {} seconds".format(run_time))

except KeyboardInterrupt:
    pass

