from data_readers import read_csv
from producer import *
import json


stream_name = '<Stream-Name>'
region = 'Region-Name'
key_id = '<Aws Key>'
secret = '<Aws Secret>'

client = create_client(region, key_id, secret)

data = read_csv('crime.csv')


send_kinesis(client, data, stream_name)

