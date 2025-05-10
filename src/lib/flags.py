class Flag:
    def __init__(self, id):
        self.encoded = id

    def __str__(self):
        flags_dict = {
            7: "CLOSE_ACK",
            6: "ERROR",
            5: "CLOSE",
            4: "LIST",
            3: "ACK",
            2: "START_SESSION",
            1: "START_SESSION_ACK",
            0: "NO_FLAGS"
        }
        return flags_dict.get(self.encoded, "UNKNOWN FLAG")

    def get_bytes(self):
        return self.encoded.to_bytes(1, byteorder='big')

    def __eq__(self, other):
        if isinstance(other, Flag):
            return self.encoded == other.encoded
        return False

    def __hash__(self):
        return hash(self.encoded)

CLOSE_ACK = Flag(7)
ERROR = Flag(6)
CLOSE = Flag(5)
LIST = Flag(4)
ACK = Flag(3)
START_SESSION = Flag(2)
START_SESSION_ACK = Flag(1)
NO_FLAGS = Flag(0)