import sys
import csv
from chord_node import Chord


def get_keys_from_csv(csv_file_name):
    """
    Process csv file and return a dictionary of keys mapped to data in each
    row of the csv.
    :param csv_file_name: name of csv file provided
    :return: key to data dictionary
    """
    data = {}
    with open(csv_file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        header_cols = next(csv_reader)
        i = 0
        for row in csv_reader:
            data[row[0] + row[3]] = list(cell for cell in row if cell != '--'
                                         and cell != '')
            i += 1
            if i == 300:
               break

    return data


def main():
    if len(sys.argv) != 3:
        print('Usage: chord_populate.py PORT FILE_NAME')
        exit(1)

    node_port = int(sys.argv[1])
    csv_file = sys.argv[2]

    data = get_keys_from_csv(csv_file)
    address = ('localhost', node_port)

    print('Asking Node {} to populate data from {} ...\n'
          .format(Chord.lookup_node(address), csv_file))
    result = Chord.populate(address, data)

    for msg in result:
        print(msg)
    print('\nThanks Node {}!'.format(Chord.lookup_node(address)))


if __name__ == '__main__':
    main()
