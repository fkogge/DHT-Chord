import socket
import threading
import sys
import pickle
import hashlib
import datetime

M = 3  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2 ** M
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n
POSSIBLE_HOSTS = ('localhost',)
POSSIBLE_PORTS = range(2 ** 16)


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
        return ''.format(self.start, self.next_start, self.node)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval


class ChordNode(object):
    def __init__(self, n):
        self.node = n
        self.finger = [None] + [FingerEntry(n, k) for k in range(1, M + 1)]  # indexing starts at 1
        self.predecessor = None
        self.keys = {}

        self.listener_address = ('localhost', TEST_BASE + n)
        self.listener = self.start_listening_server()
        print('I joined!')

    def run_server(self):
        while True:
            print('running forever')
            client_sock, client_address = self.listener.accept()
            threading.Thread(target=self.handle_rpc,
                             args=(client_address,)).start()

    def handle_rpc(self, client_sock):  # TODO figure what this is doing
        rpc = client_sock.recv(BUF_SZ)
        method, arg1, arg2 = pickle.loads(rpc)
        result = self.dispatch_rpc(method, arg1, arg2)
        client_sock.sendall(pickle.dumps(result))

    def dispatch_rpc(self, method, arg1, arg2): # TODO figure what these args are
        pass

    @property
    def successor(self):
        return self.finger[1].node

    @successor.setter
    def successor(self, id):
        self.finger[1].node = id

    def find_successor(self, id):
        """ Ask this node to find id's successor = successor(predecessor(id))"""
        np = self.find_predecessor(id)
        return self.call_rpc(np, 'successor')

    def start_listening_server(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(self.listener_address)
        listener.listen(BACKLOG)
        return listener

    def find_predecessor(self, id):
        pass

    def call_rpc(self, method_name, n_prime, param):
        pass

    def join(self, n_prime=None):
        if n_prime:
            self.init_finger_table(n_prime)
            self.update_others()
        else:
            for i in range(1, M + 1):
                self.finger[i].node = self
            self.predecessor = self

    def update_others(self):
        """ Update all other node that should have this node in their finger tables """
        # print('update_others()')
        for i in range(1,
                       M + 1):  # find last node p whose i-th finger might be this node
            # FIXME: bug in paper, have to add the 1 +
            p = self.find_predecessor((1 + self.node - 2 ** (i - 1) + NODES) % NODES)
            self.call_rpc(p, 'update_finger_table', self.node, i)

    def update_finger_table(self, s, i):
        """ if s is i-th finger of n, update this node's finger table with s """
        # FIXME: don't want e.g. [1, 1) which is the whole circle
        if (self.finger[i].start != self.finger[i].node
                # FIXME: bug in paper, [.start
                and s in ModRange(self.finger[i].start, self.finger[i].node,
                                  NODES)):
            print(
                'update_finger_table({},{}): {}[{}] = {} since {} in [{},{})'.format(
                    s, i, self.node, i, s, s, self.finger[i].start,
                    self.finger[i].node))
            self.finger[i].node = s
            print('#', self)
            p = self.predecessor  # get first node preceding myself
            self.call_rpc(p, 'update_finger_table', s, i)
            return str(self)
        else:
            return 'did nothing {}'.format(self)

    @staticmethod
    def print_time(date_time):
        """
        Printing helper for current timestamp.
        :param date_time: datetime object
        """
        return date_time.strftime('%H:%M:%S.%f')

class Chord(object):

    @staticmethod
    def lookup_node(address):
        marshalled_address = pickle.dumps(address)
        marshalled_hash = hashlib.sha1(marshalled_address).digest()
        int_hash = int.from_bytes(marshalled_hash, byteorder='big')
        bits = None
        if bits is not None:
            int_hash %= 2 ** bits
        return int_hash

def main():
    if len(sys.argv) != 2:
        print('Usage: chord_node.py NODE_PORT_NUMBER (enter 0 if '
              'starting new network)')
        exit(1)

    node_port = int(sys.argv[1])


    if node_port == 0:
        # Create new Chord node
        # Start a new network with ID = 0
        new_node = ChordNode(node_port)
        new_node.join()

    else:
        # Lookup node ID from the known Chord node port passed in

        # Hash the address of the KNOWN node port
        address_hash = hashlib.sha1(pickle.dumps((POSSIBLE_HOSTS, node_port)))
        # Get node ID of the KNOWN node
        node_id = Chord.lookup_node(address_hash)
        # Create a NEW node TODO how to figure out the next ID value to add?
        new_node = ChordNode(1)
        # Join the NEW node to the existing network using the KNOWN node ID
        new_node.join(node_id)

    new_node.run_server()


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


