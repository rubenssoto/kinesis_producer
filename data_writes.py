import json



def write_json(records):

        with open('mensagens.json', 'a') as file:
            json.dump(records, file)
            file.write('\n')

def write_txt(sequence):

    with open('checkpoint.txt', 'w') as checkpoint_file:
        checkpoint_file.write(sequence)
        checkpoint_file.close()