from producer import create_client
from consumer import *
import time


stream_name = 'rsoto_stream'
region = 'us-east-1'
key_id = 'AKIAYVE7PNVDL2VZEHM6'
secret = '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'



client = create_client(region, key_id, secret)

consumer = Kinesisconsumer(client, stream_name)

consumer.get_shard_id()
consumer.get_checkpoint()
consumer.get_shard_iterator()
consumer.get_messages()