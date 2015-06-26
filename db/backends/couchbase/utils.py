from uuid import uuid4, UUID
from base64 import b64decode, b64encode

def n1ql_escape(name):
    return '`{0}`'.format(name.replace('`', '``'))


def mk_b64(value):
    return b64encode(value.replace('/', '_'))[:-2]


def extract_b64(raw):
    return b64decode(raw.replace('_', '/') + '==')

class DocID(object):
    FMT_UUID = 'U'
    FMT_STRING = 'S'
    DELIMITER = ':'

    def __init__(self):
        self.strval = ''
        self.intval = -1

    def to_string(self):
        if not self.strval:
            raise ValueError('No string value!')
        return self.strval

    def to_int(self):
        if not self.intval:
            raise ValueError('No int value!')
        return self.intval

    @classmethod
    def decode(cls, raw):
        _, fmt, value = raw.split(cls.DELIMITER)
        obj = cls()

        if fmt == cls.FMT_STRING:
            obj.strval = value

        elif fmt == cls.FMT_UUID:
            obj.strval = value
            obj.intval = int(UUID(bytes=extract_b64(value)))
        else:
            raise Exception('Unknown format: ' + fmt)

        return obj

    @staticmethod
    def encode(table, value):
        if isinstance(value, (int, long)):
            strval = mk_b64(UUID(int=value).bytes)
            fmt = 'U'
        elif isinstance(value, UUID):
            strval = mk_b64(value.bytes)
            fmt = 'U'
        else:
            strval = value
            fmt = 'S'

        return ':'.join([table, fmt, strval])

    @staticmethod
    def generate(table):
        return DocID.encode(table, uuid4())
