import boto3
import time
from data_writes import write_json, write_txt
from data_readers import read_txt
import json

class Message:

    def __init__(self, data):
        self.data = data


class Kinesisconsumer:

    def __init__(self, client,stream_name):

        self.client = client
        self.shard_id = None
        self.shard_iterator = None
        self.stream_name = stream_name
        self.records_list = []
        self.sequence = None

    def get_shard_id(self):

        shards = self.client.describe_stream(StreamName=self.stream_name)
        self.shard_id = shards['StreamDescription']['Shards'][0]['ShardId']

    def get_checkpoint(self):

        sequence_number = read_txt()
        self.sequence = sequence_number[0]

    def get_shard_iterator(self):

         shard_iterator = self.client.get_shard_iterator(StreamName=self.stream_name, ShardId=self.shard_id, ShardIteratorType='AFTER_SEQUENCE_NUMBER', StartingSequenceNumber=self.sequence)

         self.shard_iterator = shard_iterator['ShardIterator']

    def deduplicate_messages(self, message):

        for row in message['Records']:

            row_dict = json.loads(row['Data'].decode('utf-8'))
            self.records_list.append(row_dict)
            self.sequence = row['SequenceNumber']

    def get_messages(self):

        record = self.client.get_records(ShardIterator=self.shard_iterator, Limit=2)

        while 'NextShardIterator' in record:
            record = self.client.get_records(ShardIterator=self.shard_iterator, Limit=200)
            self.shard_iterator = record['NextShardIterator']

            self.deduplicate_messages(record)

            if len(self.records_list) >= 1000:
                try:
                    write_json(self.records_list)
                    self.records_list = []
                    write_txt(self.sequence)
                except:
                    print('erro')





