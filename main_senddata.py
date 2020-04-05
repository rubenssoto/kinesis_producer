from data_readers import read_csv
from producer import *
from acumulator import acumulator
import json

stream_name = 'rsoto_stream'
region = 'us-east-1'
key_id = 'AKIAYVE7PNVDL2VZEHM6'
secret = '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'
num_threads = 5
batch_size = 100
csv_to_send = 'crime.csv'


client = create_client(region, key_id, secret)

data = read_csv(csv_to_send)

line = acumulator(data, batch_size)

k = Kinesisproducer(client, data, stream_name, line, num_threads)
k.multithread()

line.join()