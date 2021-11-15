import sys
import csv
from chord_node import Chord

def get_keys_from_csv(csv_file_name):
    # keys = []
    data = {}
    with open(csv_file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader)
        for row in csv_reader:
            print('{} {}'.format(row[0], row[3]))
            # keys.append(row[0] + str(row[3]))
            data[row[0] + row[3]] = list(cell for cell in row if cell != '--'
                                         and cell != '')
    #return keys
    return data

def main():
    if len(sys.argv) != 3:
        print('Usage: chord_populate.py PORT FILE_NAME')
        exit(1)

    node_port = int(sys.argv[1])
    csv_file = sys.argv[2]

    keys = get_keys_from_csv(csv_file)
    address = ('localhost', node_port)
    print(keys)

    # data_added = Chord.populate(address, keys)
    # for data in data_added:
    #     print('{} added to Chord network.'.format(data))
    # print('Thanks Node {}!'.format(Chord.lookup_node(address)))


if __name__ == '__main__':
    main()