from data_readers import read_csv
from producer import *
from acumulator import acumulator
import json


stream_name = '<stream_name>'
region = '<region name>'
key_id = '<key id>'
secret = '<key secret>'
num_threads = 5
batch_size = 100
csv_to_send = 'crime.csv'


client = create_client(region, key_id, secret)

data = read_csv(csv_to_send)

line = acumulator(data, batch_size)

k = Kinesisproducer(client, data, stream_name, line, num_threads)
k.multithread()

line.join()