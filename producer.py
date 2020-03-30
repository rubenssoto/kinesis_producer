import boto3
import json
from acumulator import acumulator
from threading import Thread

def create_client(region, key_id, secret):
    kinesis_client = boto3.client('kinesis', region_name = region, aws_access_key_id=key_id, aws_secret_access_key=secret)
    return kinesis_client


class Kinesisproducer:

    def __init__(self, kinesis_client, data, stream_name, line, num_threads):
        self.kinesis_client = kinesis_client
        self.data = data
        self.stream_name = stream_name
        self.line = line
        self.num_threads = num_threads

    def send_kinesis(self):
        send_to_kinesis = True

        while send_to_kinesis == True:

            try:
                kinesis_records = self.line.get_nowait()
                response = self.kinesis_client.put_records(
                                        Records = kinesis_records,
                                        StreamName = self.stream_name
                                     )
                print('Event Sended')
            except:
                send_to_kinesis = False




    def multithread(self):

        for i in range(self.num_threads):
            worker = Thread(target=self.send_kinesis)
            worker.setDaemon(True)
            worker.start()







