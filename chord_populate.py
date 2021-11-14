import sys
import csv
from chord_node import Chord

def get_keys_from_csv(csv_file_name):
    keys = []
    with open(csv_file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader)
        for row in csv_reader:
            print('{} {}'.format(row[0], row[3]))
        keys.append(row[0] + row[3])
    return keys

def main():
    if len(sys.argv) != 3:
        print('Usage: chord_populate.py PORT FILE_NAME')
        exit(1)

    node_port = int(sys.argv[1])
    csv_file = sys.argv[2]

    keys = get_keys_from_csv(csv_file)
    address = ('localhost', node_port)

    Chord.populate(address, keys)


if __name__ == '__main__':
    main()