import sys
from chord_node import Chord


def main():
    if len(sys.argv) != 3:
        print('Usage: chord_query.py NODE_PORT KEY (playerID + year)')
        exit(1)

    address = ('localhost', int(sys.argv[1]))
    key = sys.argv[2]
    print('Asking Node {} to lookup up data for key = \'{}\''
          .format(Chord.lookup_node(address), key))

    data_list = Chord.lookup_key(address, key)
    for data in data_list:
        print(data)


if __name__ == '__main__':
    main()