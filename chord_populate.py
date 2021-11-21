"""
Ask a single node in the Chord network to populate the network with data from
a csv file

Author: Francis Kogge
Date: 11/20/2021
Course: CPSC 5520
"""

from chord_node import Chord
import sys
import csv

MAX_ROW = 2000  # Number of rows from the CSV to process


def get_keys_from_csv(csv_file_name, max_row=None):
    """
    Process csv file and return a dictionary of keys mapped to data in each
    row of the csv.
    :param csv_file_name: name of csv file provided
    :param max_row: number of rows to process (entire csv if max=None)
    :return: key to data dictionary
    """
    data = {}
    with open(csv_file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        header_row = next(csv_reader)

        i = 0
        for row in csv_reader:
            i += 1
            key = row[0] + row[3]
            data[key] = []
            for header, cell in zip(header_row, row):
                if cell != '--' and cell != '':
                    data[key].append((header, cell))

            # If max row specified, return after max row number is processed
            if max_row:
                if i == max_row:
                    return data

    return data


def main():
    """
    Executes program from the main entry point.
    """
    # Expects 2 additional arguments
    if len(sys.argv) != 3:
        print('Usage: chord_populate.py PORT FILE_NAME')
        exit(1)

    node_port = int(sys.argv[1])
    csv_file = sys.argv[2]

    data = get_keys_from_csv(csv_file, MAX_ROW)
    address = ('localhost', node_port)

    print('Asking Node {} to populate data from {} ...\n'
          .format(Chord.lookup_node(address), csv_file))
    result = Chord.populate(address, data)

    if result[0]:
        for msg in result:
            print(msg)
        print('\nThanks Node {}!'.format(Chord.lookup_node(address)))


if __name__ == '__main__':
    main()
