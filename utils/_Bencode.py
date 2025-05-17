from collections import OrderedDict
import time

INTEGER_TOKEN = b'i'
LIST_TOKEN = b'l'
DICT_TOKEN = b'd'
END_TOKEN = b'e'
SEPARATOR_TOKEN = b':'


class Decoder:
    """
    Decoder class that takes in Bencoded data and decodes it.
    """
    def __init__(self, data):
        if not isinstance(data, bytes):
            raise TypeError('Argument data must be of type bytes, it is of type ' + str(type(data)))
        self._data = data
        self._index = 0

    def __repr__(self):
        return 'Decoder()'

    def _peek(self):
        if self._index >= len(self._data):  # in bounds
            return None
        return self._data[self._index:self._index + 1]

    def _consume(self):
        self._index += 1

    def _read(self, length):
        if length < 0:
            raise ValueError("Length must be non-negative")
        if self._index + length > len(self._data):
            raise IndexError(f"Requested {length} bytes at index {self._index}, "
                             f"but data has only {len(self._data)} bytes")
        results = self._data[self._index:self._index + length]
        self._index += length
        return results

    def _read_until_token(self, token):
        try:
            if self._index >= len(self._data):
                raise RuntimeError("Reached end of data")

            occurrence = self._data.find(token, self._index)

            if occurrence == -1:
                raise RuntimeError(f"Token {token} not found")

            results = self._data[self._index: occurrence]
            self._index = occurrence + 1

            return results
        except ValueError:
            raise RuntimeError('Unable to find token {0}'.format(str(token)))

    def _decode_int(self):
        number = self._read_until_token(END_TOKEN)
        return int(number)

    def _decode_list(self):
        results = []
        # recursively call decode to add elements to results
        while self._peek() != END_TOKEN:
            results.append(self.decode())
        self._consume()
        return results

    def _decode_dict(self):
        results = OrderedDict()
        while self._data[self._index: self._index + 1] != END_TOKEN:
            key = self.decode()
            obj = self.decode()
            results[key] = obj
        self._consume()  # The END token
        return results

    def _decode_string(self):
        length_of_string = int(self._read_until_token(SEPARATOR_TOKEN))
        data = self._read(length_of_string)
        return data

    def decode(self):
        c = self._peek()
        if c is None:
            raise EOFError('Unexpected Eof')
        elif c == INTEGER_TOKEN:
            self._consume()
            return self._decode_int()
        elif c == LIST_TOKEN:
            self._consume()
            return self._decode_list()
        elif c == DICT_TOKEN:
            self._consume()
            return self._decode_dict()
        elif c == END_TOKEN:
            return None
        elif c in b'0123456789':
            return self._decode_string()
        else:
            raise RuntimeError('Invalid token read at {0}'.format(
                str(self._index)))


class Encoder:
    """
    Bencoder. takes in int or list or dict and returns Bencoded bytearray
    """
    def __init__(self, data):
        self._data = data

    def __repr__(self):
        return 'Encoder()'

    def encode(self):
        return self._encode(self._data)

    def _encode(self, data):
        if isinstance(data, str):
            encoded = data.encode('utf-8')
            return str(len(encoded)).encode('utf-8') + SEPARATOR_TOKEN + encoded

        elif isinstance(data, bytes):
            return str(len(data)).encode('utf-8') + SEPARATOR_TOKEN + data

        elif isinstance(data, int):
            return INTEGER_TOKEN + str(data).encode('utf-8') + END_TOKEN

        elif isinstance(data, list):
            return LIST_TOKEN + b''.join([self._encode(item) for item in data]) + END_TOKEN

        elif isinstance(data, dict):
            # Keys in Bencode dicts must be sorted and encoded as bytes
            items = []
            for key in sorted(data.keys()):
                key_encoded = self._encode(key if isinstance(key, (str, bytes)) else str(key))
                value_encoded = self._encode(data[key])
                items.append(key_encoded + value_encoded)
            return DICT_TOKEN + b''.join(items) + END_TOKEN

        else:
            raise TypeError(f"Unsupported type: {type(data)}")


def _format_bytes(size):
    """Convert bytes to a human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


def print_torrent(data, indent=0, max_string_length=80, max_pieces_display=20):
    """Recursively print torrent metadata with proper formatting."""
    indent_str = ' ' * indent

    if isinstance(data, dict):
        print(f"{indent_str}{{")
        for key, value in data.items():
            key_str = key.decode('utf-8', errors='replace') if isinstance(key, bytes) else str(key)
            print(f"{indent_str}  {key_str}: ", end='')

            # Handle special keys
            if key in (b'pieces', 'pieces') and isinstance(value, bytes):
                print(f"[{len(value)} bytes of hash data]")
                if len(value) >= 20:
                    hex_str = value[:40].hex()
                    spaced_hex = ' '.join(hex_str[i:i + 2] for i in range(0, len(hex_str), 2))
                    print(f"{indent_str}    (First 40 bytes: {spaced_hex}...)")
            elif key in (b'creation date', 'creation date') and isinstance(value, int):
                print(f"{value} ({time.ctime(value)})")
            elif key in (b'length', 'length') and isinstance(value, int):
                print(f"{value} ({_format_bytes(value)})")
            else:
                print_torrent(value, indent + 4, max_string_length, max_pieces_display)
        print(f"{indent_str}}}")

    elif isinstance(data, list):
        print(f"{indent_str}[")
        for i, item in enumerate(data):
            if i >= max_pieces_display:
                print(f"{indent_str}  ...{len(data) - max_pieces_display} more items...")
                break
            print_torrent(item, indent + 4, max_string_length, max_pieces_display)
        print(f"{indent_str}]")

    elif isinstance(data, (bytes, str)):
        data_str = data.decode('utf-8', errors='replace') if isinstance(data, bytes) else data
        if len(data_str) > max_string_length:
            truncated = data_str[:max_string_length] + f"... [truncated, total {len(data_str)} chars]"
            print(f"{indent_str}{repr(truncated)}")
        else:
            print(f"{indent_str}{repr(data_str)}")

    else:
        print(f"{indent_str}{data}")


