import random
import socket
import threading
import sys
import pickle
import hashlib
from datetime import datetime
from enum import Enum
from random import randrange

M = 4  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2 ** M
BUF_SZ = 8192  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n
MIN_PORT = 50000
DEFAULT_HOST = 'localhost'
POSSIBLE_PORTS = range(2 ** 16)
RPC_TIMEOUT = 3


class ModRange(object):
    """
    Range-like object that wraps around 0 at some divisor using modulo arithmetic.

    >>> mr = ModRange(1, 4, 100)
    >>> mr
    <mrange [1,4)%100>
    >>> 1 in mr and 2 in mr and 4 not in mr
    True
    >>> [i for i in mr]
    [1, 2, 3]
    >>> mr = ModRange(97, 2, 100)
    >>> 0 in mr and 99 in mr and 2 not in mr and 97 in mr
    True
    >>> [i for i in mr]
    [97, 98, 99, 0, 1]
    >>> [i for i in ModRange(0, 0, 5)]
    [0, 1, 2, 3, 4]
    """

    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around
        # the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor),)
        else:
            self.intervals = (
            range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)


class ModRangeIter(object):
    """ Iterator class for ModRange """

    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]


class FingerEntry(object):
    """
    Row in a finger table.

    >>> fe = FingerEntry(0, 1)
    >>> fe

    >>> fe.node = 1
    >>> fe

    >>> 1 in fe, 2 in fe
    (True, False)
    >>> FingerEntry(0, 2, 3), FingerEntry(0, 3, 0)
    (, )
    >>> FingerEntry(3, 1, 0), FingerEntry(3, 2, 0), FingerEntry(3, 3, 0)
    (, , )
    >>> fe = FingerEntry(3, 3, 0)
    >>> 7 in fe and 0 in fe and 2 in fe and 3 not in fe
    True
    """

    def __init__(self, n, k, node=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2 ** (k - 1)) % NODES
        self.next_start = (n + 2 ** k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return '{:<2} | [{:<2}, {:<2}) | {}'.format(self.start, self.start,
                                           self.next_start, self.node)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval


class RPC(Enum):
    """
    Remote procedure call (RPC) enum class for organizing names of methods that
    can be invoked on other nodes in the network via an RPC call.
    """
    FIND_SUCCESSOR = 'find_successor'
    FIND_PREDECESSOR = 'find_predecessor'
    CLOSEST_PRECEDING_FINGER = 'closest_preceding_finger'
    SUCCESSOR = 'successor'
    UPDATE_FINGER_TABLE = 'update_finger_table'
    SET_PREDECESSOR = 'set_predecessor'
    GET_PREDECESSOR = 'get_predecessor'
    ADD_KEY = 'add_key'
    GET_DATA = 'get_data'


class ChordNode(object):
    def __init__(self, n):
        self.node = n  # Identifier of this node
        self.finger = [None] + [FingerEntry(n, k) for k in range(1, M + 1)]  # index i of finger table: 1 <= i <= M
        self.predecessor = None
        self.keys = {}

        self.listener_address = (DEFAULT_HOST, Chord.get_empty_port(self.node))
        self.listener = self.start_listening_server()
        print('Node ID = {} joined on {} at [{}]'
              .format(self.node, self.listener_address, Chord.print_time()))

    def init_key_map(self):
        pass
        #self.keys = {key: None for key in ModRange(self.predecessor + 1, self.node + 1, NODES)}

    def update_key_map(self):
        self.keys = {key: self.keys[key] for key in ModRange(self.predecessor + 1, self.node + 1, NODES)}

    def run_server(self):
        while True:
            print(self.print_neighbors())
            print('\nWaiting for incoming connection...\n')
            client_sock, client_address = self.listener.accept()
            threading.Thread(target=self.handle_rpc, args=(client_sock,)).start()

    def handle_rpc(self, client_sock):
        rpc = client_sock.recv(BUF_SZ)
        method, arg1, arg2 = pickle.loads(rpc)
        print('Received RPC request: \"{}\"'.format(method))
        result = self.dispatch_rpc(method, arg1, arg2)
        client_sock.sendall(pickle.dumps(result))

    def dispatch_rpc(self, method, arg1=None, arg2=None):
        if method == RPC.FIND_SUCCESSOR.value:
            return self.find_successor(arg1)

        elif method == RPC.SUCCESSOR.value:
            if arg1:
                self.successor(arg1)
            else:
                return self.successor

        elif method == RPC.CLOSEST_PRECEDING_FINGER.value:
            return self.closest_preceding_finger(arg1)

        elif method == RPC.UPDATE_FINGER_TABLE.value:
            print(self.update_finger_table(arg1, arg2))
            self.update_keys()

        elif method == RPC.SET_PREDECESSOR.value:
            self.set_predecessor(arg1)

        elif method == RPC.GET_PREDECESSOR.value:
            return self.get_predecessor()

        elif method == RPC.ADD_KEY.value:
            return self.add_key(arg1, arg2)

        elif method == RPC.GET_DATA.value:
            return self.get_data(arg1)

        return 'no return value'

    def update_keys(self, new_key=None, new_data=None):
        if new_key and new_data:
            self.keys[new_key] = new_data
            return

        remove_list = []
        for key, data in self.keys.items():
            if key not in ModRange(self.predecessor + 1, self.successor + 1, NODES):
                remove_list.append(key)
                n_prime = self.find_successor(key)
                self.call_rpc(n_prime, RPC.UPDATE_KEYS, key, data)

        for key in remove_list:
            del self.keys[key]


    def call_rpc(self, n_prime, method: RPC, arg1=None, arg2=None):
        method_name = method.value
        # If RPC requested on myself, then just do a local call
        if n_prime == self.node:
            return self.dispatch_rpc(method_name, arg1, arg2)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as n_prime_sock:
            n_prime_address = Chord.lookup_address(n_prime)
            n_prime_sock.settimeout(RPC_TIMEOUT)
            try:
                n_prime_sock.connect(n_prime_address)
                marshalled_data = pickle.dumps((method_name, arg1, arg2))
                n_prime_sock.sendall(marshalled_data)
                return pickle.loads(n_prime_sock.recv(BUF_SZ))

            except Exception as e:
                print('Failed to connect to Node ID = {}, RPC aborted at [{}]'
                      .format(n_prime, Chord.print_time()))
                return None

    @property
    def successor(self):
        return self.finger[1].node

    @successor.setter
    def successor(self, id):
        self.finger[1].node = id

    def set_predecessor(self, node):
        self.predecessor = node

    def get_predecessor(self):
        return self.predecessor

    def find_successor(self, id):
        """ Ask this node to find id's successor = successor(predecessor(id))"""
        n_prime = self.find_predecessor(id)
        return self.call_rpc(n_prime, RPC.SUCCESSOR)

    def find_predecessor(self, id):
        n_prime = self.node
        while id not in ModRange(n_prime + 1, self.call_rpc(n_prime, RPC.SUCCESSOR) + 1, NODES):
            n_prime = self.call_rpc(n_prime, RPC.CLOSEST_PRECEDING_FINGER, id)
        return n_prime

    def closest_preceding_finger(self, id):
        for i in range(M, 0, -1):
            if self.finger[i].node in ModRange(self.node + 1, id, NODES):
                return self.finger[i].node
        return self.node

    def join(self, n_prime=None):
        if n_prime:
            self.init_finger_table(n_prime)
            self.update_others()
        else:
            for i in range(1, M + 1):
                self.finger[i].node = self.node
            self.predecessor = self.node
        #self.init_key_map()
        print('Joined network at [{}]'.format(Chord.print_time()))
        print(self.print_finger_table())



    def init_finger_table(self, n_prime):
        self.finger[1].node = self.call_rpc(n_prime, RPC.FIND_SUCCESSOR,
                                            self.finger[1].start)

        # Pseudocode: predecessor = successor.predecessor;
        self.predecessor = self.call_rpc(self.successor, RPC.GET_PREDECESSOR)
        # Pseudocode: successor.predecessor = n;
        self.call_rpc(self.successor, RPC.SET_PREDECESSOR, self.node)


        for i in range(1, M):
            if self.finger[i + 1].start in ModRange(self.node,
                                                    self.finger[i].node, NODES):
                self.finger[i + 1].node = self.finger[i].node
            else:
                self.finger[i + 1].node = \
                    self.call_rpc(n_prime, RPC.FIND_SUCCESSOR,
                                  self.finger[i + 1].start)
        print('Initialize finger table complete at [{}]'
              .format(Chord.print_time()))

    def update_others(self):
        """
        Update all other node that should have this node in their
        finger tables
        """
        # find last node p whose i-th finger might be this node
        for i in range(1, M + 1):
            # FIXME: bug in paper, have to add the 1 +
            p = self.find_predecessor((1 + self.node - 2 ** (i - 1) + NODES) % NODES)
            self.call_rpc(p, RPC.UPDATE_FINGER_TABLE, self.node, i)

    def update_finger_table(self, s, i):
        """ if s is i-th finger of n, update this node's finger table with s """
        # FIXME: don't want e.g. [1, 1) which is the whole circle
        # FIXME: bug in paper, [.start
        if (self.finger[i].start != self.finger[i].node
                and s in ModRange(self.finger[i].start,
                                  self.finger[i].node, NODES)):
            print('update_finger_table({},{}): {}[{}] = {} since {} in [{},{})'
                  .format(s, i, self.node, i, s, s, self.finger[i].start,
                          self.finger[i].node))
            self.finger[i].node = s
            print('#', self)
            p = self.predecessor  # get first node preceding myself
            self.call_rpc(p, RPC.UPDATE_FINGER_TABLE, s, i)
            #self.update_key_map()
            #print(self.keys)
            print(self.print_finger_table())
            return str(self)
        else:
            #self.update_key_map()
            #print(self.keys)
            print(self.print_finger_table())
            return 'did nothing {}'.format(self)

    def __repr__(self):
        return str(self.node)

    def start_listening_server(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(self.listener_address)
        listener.listen(BACKLOG)
        return listener

    def print_finger_table(self):
        """Prints the finger table contents."""
        return '\n'.join(str(row) for row in self.finger[1:])

    def add_key(self, key, data):
        """
        Adds the data to the key map. Recursively finds successor of the key
        through RPC calls. The data maps to an M-bit key, defined by the
        identifier space, so no key ID shall be >= NODES.
        :param key:
        :param data:
        :return:
        """
        if key >= NODES:
            raise ValueError('Error: maximum ID allowed is {}'
                             .format(NODES - 1))

        if key in ModRange(self.predecessor + 1, self.node + 1, NODES):
            self.keys[key] = data
            print(self.keys)
            return 'Node {} added key {}'.format(self.node, key)
        else:
            # Find key successor
            n_prime = self.find_successor(key)
            return self.call_rpc(n_prime, RPC.ADD_KEY, key, data)

    def get_data(self, key):
        """
        Get data associated with the key.
        :param key: key to lookup
        :return: data
        """
        if key >= NODES:
            raise ValueError('Error: maximum ID stored is {}'.format(NODES - 1))

        if key in ModRange(self.predecessor + 1, self.node + 1, NODES):
            return self.keys[key]
        else:
            # Find key successor
            n_prime = self.find_successor(key)
            return self.call_rpc(n_prime, RPC.GET_DATA, key)

    def print_neighbors(self):
        return 'predecessor: {}\nsucessor: {}'.format(self.predecessor,
                                                      self.successor)

class Chord(object):

    node_map = {}  # Key: node, Value: list of ports

    @staticmethod
    def contact_node(address: tuple[str, int], method: RPC, key_map=None,
                     key=None):
        """
        Helper method for either populating data to or retrieving data from the
        node at the given address in the chord network Chord  network.
        - If key_map is supplied, an RPC request for ADD_KEY is made: each key
          gets added to the network
        - If key is supplied, an RPC request for GET_DATA is made: the data is
          mapped to the given key is retrieved

        :param address: (host, port) address pair of a node
        :param method: RPC method to call
        :param key_map: dictionary of key/data pairs to add
        :param key: key to retrieve data from
        :return: list of data retrieved from the chord node
        """
        if method != RPC.ADD_KEY and method != RPC.GET_DATA:
            raise ValueError('Only \'{}\' or \'{}\' RPC requests are permitted.'
                             .format(RPC.ADD_KEY.value, RPC.GET_DATA.value))

        if key:
            key_map = {key: None}

        data_retrieved = []

        for key, data in key_map.items():
            marshalled_key = pickle.dumps(key)
            marshalled_hash = hashlib.sha1(marshalled_key).digest()
            key_id = int.from_bytes(marshalled_hash, byteorder='big') % NODES

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                try:
                    sock.connect(address)
                    marshalled_data = pickle.dumps((method.value, key_id, data))
                    sock.sendall(marshalled_data)

                except Exception as e:
                    print('RPC request \'{}\' failed at {}.'
                          .format(method.value, Chord.print_time()))

                else:
                    data_retrieved.append(pickle.loads(sock.recv(BUF_SZ)))

        return data_retrieved

    @staticmethod
    def populate(address: tuple[str, int], keys: dict):
        """
        Populate
        :param address:
        :param keys:
        :return:
        """
        return Chord.contact_node(address, RPC.ADD_KEY, key_map=keys)

    @staticmethod
    def lookup_key(address: tuple[str, int], key: str):
        return Chord.contact_node(address, RPC.GET_DATA, key=key)[0]

    @staticmethod
    def generate_node_map():
        """
        Hash all possible IP addresses, assuming we're using local host, to
        possible M-bit ID matches. Map all of the possible IDs to the port
        number pair.
        :return:
        """
        for n in range(NODES):
            Chord.node_map[n] = []

        for port in range(MIN_PORT, 2 ** 16):
            node = Chord.lookup_node((DEFAULT_HOST, port))
            Chord.node_map[node].append(port)

    @staticmethod
    def lookup_address(node):
        print(node)
        port_list = Chord.node_map[node]
        for port in port_list:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                address = (DEFAULT_HOST, port)
                try:
                    sock.bind(address)

                except Exception as e:
                        # If can't bind that, means a node is using it
                    print('Found Node {} on {} at [{}]'
                          .format(node, address, Chord.print_time()))
                    return address

    # if node_map was the other way around (port key to id value)


    @staticmethod
    def lookup_node(address):
        """

        :param
        :return: M-bit node ID
        """
        # for port in POSSIBLE_PORTS:
        marshalled_address = pickle.dumps(address)
        marshalled_hash = hashlib.sha1(marshalled_address).digest()
        unmarshalled_hash = int.from_bytes(marshalled_hash, byteorder='big')
        return unmarshalled_hash % NODES  # 2^M

    @staticmethod
    def get_empty_port(node_id):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            port_list = Chord.node_map[node_id]
            for port in port_list:
                address = (DEFAULT_HOST, port)

                try:
                    sock.bind(address)
                except Exception as e:
                    print("Port {} in use.".format(port))
                else:
                    return port

    @staticmethod
    def print_time():
        """
        Printing helper for current timestamp.
        :param date_time: datetime object
        """
        return datetime.now().strftime('%H:%M:%S.%f')


def main():
    if len(sys.argv) != 2:
        print('Usage: chord_node.py NODE_PORT_NUMBER (enter 0 if '
              'starting new network)')
        Chord.generate_node_map()
        for i in range(M, 0, -1):
            print(i)
        print(Chord.node_map)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 55321))
        sock.close() # If close first, then i can rebind port 55321 to a new socket
        sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock2.bind(('localhost', 55321))

        sock2.close()

        exit(1)

    Chord.generate_node_map()
    node_port = int(sys.argv[1])
    known_node_id = None

    # Create new Chord network
    if node_port == 0:
        # Get any random m-bit ID
        node = randrange(0, NODES)
        #join()

    else:
        known_node_id = Chord.lookup_node(('localhost', node_port))
        node = known_node_id
        while node == known_node_id:
            node = randrange(0, NODES)
        #join(known_node_id)

    new_node = ChordNode(node)
    new_node.join(known_node_id)
    new_node.run_server()

        # Lookup node ID from the known Chord node port passed in

        # Hash the address of the KNOWN node port
        #address_hash = hashlib.sha1(pickle.dumps((POSSIBLE_HOSTS[0], node_port)))
        # Get node ID of the KNOWN node

        #known_node_id = Chord.lookup_node(POSSIBLE_HOSTS[0] + node_port)
        # Create a NEW node TODO how to figure out the next ID value to add?
        #new_node = ChordNode(node_id)
        # Join the NEW node to the existing network using the KNOWN node ID
        #new_node.join(known_node_id)




    # if node_port == 0:
    #     # Start a new network
    #     node = ChordNode(node_id)
    #     node.run_server()
    # else:
    #     # Join the existing network with known node port number
    #     # TODO create new node -
    #     pass





if __name__ == '__main__':
    main()


