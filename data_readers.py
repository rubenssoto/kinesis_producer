import csv

def read_csv(filepath):
    with open('crime.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        dict_list = []

        for dict_row in reader:
            dict_list.append(dict(dict_row))

    return dict_list

def read_txt():
    with open('checkpoint.txt', 'r') as txtfile:
        line = txtfile.readlines()
    return line


