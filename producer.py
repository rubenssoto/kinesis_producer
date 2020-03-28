import boto3
import json


def create_client(region, key_id, secret):
    kinesis_client = boto3.client('kinesis', region_name = region, aws_access_key_id=key_id, aws_secret_access_key=secret)
    return kinesis_client

def send_kinesis(kinesis_client, data, stream_name):

    kinesis_records = []

    send_to_kinesis = False

    response = ''

    eventscount = 0

    currentBytes = 0

    for row in data:

        kinesis_records.append({"Data": json.dumps(row),
                                "PartitionKey": row['district']})

        currentBytes = len(str(kinesis_records).encode('utf-8'))

        if len(kinesis_records) == 500 or currentBytes >= 800000:
            send_to_kinesis = True
            eventscount = eventscount + len(kinesis_records)

        if send_to_kinesis == True:
            response = kinesis_client.put_records(
                                    Records = kinesis_records,
                                    StreamName = stream_name
                                 )

            kinesis_records = []

            send_to_kinesis = False

            currentBytes = 0

            print(f'Sended {eventscount} Events')




