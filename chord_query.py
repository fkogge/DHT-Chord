"""
Query a single node in the Chord network for a key from the csv file.

Author: Francis Kogge
Date: 11/20/2021
Course: CPSC 5520
"""

from chord_node import Chord
import sys


def print_data(key, data_list):
    """
    Print the data list.
    :param key: Key associated with the data
    :param data_list: data elements
    """
    if data_list:
        id = data_list[0][1]
        year = data_list[2][1] if data_list[2][0] == 'Year' else data_list[3][1]
        retrieved_key = id + year

        if retrieved_key == key:
            for label, data in data_list:
                print('{}: {}'.format(label, data))
        else:
            print('Key hash collision: key \'{}\' does not match retrieved '
                  'key \'{}\''.format(key, retrieved_key))
    else:
        print('No data found.')


def main():
    """
    Executes program from the main entry point.
    """
    # Expects 2 additional arguments
    if len(sys.argv) != 3:
        print('Usage: chord_query.py NODE_PORT KEY (playerID + year)')
        exit(1)

    address = ('localhost', int(sys.argv[1]))
    key = sys.argv[2]
    print('Asking Node {} to lookup up data for key = \'{}\' ...\n'
          .format(Chord.lookup_node(address), key))

    data_list = Chord.lookup_key(address, key)
    print_data(key, data_list)


if __name__ == '__main__':
    main()
