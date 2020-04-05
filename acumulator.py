import queue
import json

def acumulator(data, maxsize):

    data_list = []
    line = queue.Queue(maxsize=0)

    for row in data:

        if len(data_list) < maxsize:
            data_list.append({"Data": bytes(str(json.dumps(row)), 'utf-8'),
                                "PartitionKey": row['district']})
        else:
            line.put(data_list)
            data_list = []

    return line

