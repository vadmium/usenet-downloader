from shorthand import bitmask
from contextlib import ExitStack, closing
from shorthand import chunks
from coropipe import PipeWriter
import re
from io import BufferedIOBase
from log import Progress
from warnings import warn

class FileDecoder:
    def __init__(self, log, pipe):
        self.log = log
        self.pipe = pipe
    
    def parse_header(self):
        header = dict()
        # TODO: skip data until $=ybegin>; limit skipped data to say 1000 B
        # TODO flexible ordering; handle omission; flexible space; ignore lines
        # TODO: handle extra parameters
        # TODO: possibility to emit notices about unhandled parameters
        try:
            while True:
                stripped = self.pipe.buffer.lstrip(b"\r\n")
                if stripped:
                    break
                self.pipe.buffer = yield
            self.pipe.buffer = stripped
            
            yield from self.pipe.expect(b"=ybegin part=")
        except EOFError:
            return None
        [header["part"], _] = yield from self.pipe.read_delimited(b" ",
            self.PART_DIGITS)
        header["part"] = int(header["part"]) - 1
        if (yield from self.pipe.consume_match(b"total=")):
            [header["total"], _] = yield from self.pipe.read_delimited(
                b" ", self.PART_DIGITS)
            header["total"] = int(header["total"])
        yield from self.pipe.consume_match(b"line=128 ")
        yield from self.pipe.expect(b"size=")
        [header["size"], _] = yield from self.pipe.read_delimited(b" ",
            self.SIZE_DIGITS)
        header["size"] = int(header["size"])
        yield from self.pipe.consume_match(b"line=128 ")
        
        yield from self.pipe.expect(b"name=")
        [header["name"], _] = yield from self.pipe.read_delimited(b"\n",
            self.NAME_CHARS)
        header["name"] = header["name"].rstrip()
        if (header["name"].startswith(b'"') and
        header["name"].endswith(b'"') and header["name"] != b'"'):
            header["name"] = header["name"][1:-1]
        header["name"] = header["name"].decode("ascii")
        
        if (yield from self.pipe.consume_match(b"=ypart begin=")):
            [header["begin"], _] = yield from self.pipe.read_delimited(b" ",
                self.SIZE_DIGITS)
            header["begin"] = int(header["begin"]) - 1
            yield from self.pipe.expect(b"end=")
            [header["end"], _] = yield from self.pipe.read_delimited(b"\n",
                self.SIZE_DIGITS)
            header["end"] = int(header["end"])
        # TODO: make sure part size is not ridiculously huge
        return header
    
    def validate_header(self, header, chunking):
        # todo update params and compare
        
        size = header.get("size")
        total = header.get("total")
        if (size is not None and total is not None and
        chunks(size, chunking) != total):
            raise ValueError(header)
        
        [number, remainder] = divmod(header["begin"], chunking)
        # TODO: compare number with part
        if total is not None and number >= total or remainder:
            raise ValueError(header["begin"])
        if total is None:
            # TODO: part size <= chunking
            pass
        else:
            if number == total - 1:
                expected = size
            else:
                expected = header["begin"] + chunking
            if expected not in (header["end"], None):
                raise ValueError(header["end"])
    
    def decode_part(self, file, header):
        # TODO: limit decoded data to (end - begin), or size if not partial, or some hard-coded limit if no size given
        file.seek(header["begin"])
        # TODO: do not allow =y lines, newlines, etc to exceed data bytes by say 100
        progress = Progress(self.log, header["size"], header["begin"])
        with closing(StreamDecoder(UnclosingWriter(file))) as decoder:
            while True:
                data = self.pipe.buffer
                keywords = self.KEYWORD_LINE.search(data)
                if keywords:
                    data = data[:keywords.start()]
                else:
                    escape = self.HALF_ESCAPE.search(data)
                    if escape:
                        data = data[:escape.start()]
                decoder.feed(data)
                decoder.flush()
                if keywords:
                    # TODO: ignore any unknown =y... lines
                    self.pipe.buffer = self.pipe.buffer[keywords.end():]
                    break
                if escape:
                    while True:
                        self.pipe.buffer = (yield).lstrip(b"\r\n")
                        if self.pipe.buffer:
                            break
                    if self.pipe.buffer.startswith(b"y"):
                        self.pipe.buffer = self.pipe.buffer[1:]
                        break
                else:
                    self.pipe.buffer = yield
                    progress.update(file.tell())
                    # TODO: incorporate into total of all files; update total of all files with real file size
                    # TODO: keep samples over multiple parts
        if file.tell() != header["end"]:
            raise ValueError(header["end"])
        
        expected = "end size={} part={} pcrc32="
        size = header["end"] - header["begin"]
        expected = expected.format(size, 1 + header["part"])
        yield from self.pipe.expect(expected.encode("ascii"))
        crc = decoder.getCrc32()
        [stated, delim] = yield from self.pipe.read_delimited(b" \r\n", 8)
        if int(stated, 16) != int(crc, 16):
            msg = "Calculated part CRC {} != stated pcrc32={}"
            raise ValueError(msg.format(crc, stated))
        
        if delim != b"\n":
            yield from self.pipe.read_delimited(b"\n", 30)
        
        # TODO: explicitly detect second yEnc object and report as error,
        # since this is specifically allowed
    
    PART_DIGITS = 10  # yEnc1-formal1.txt (2002) maximum is 999 (3 digits)
    SIZE_DIGITS = 30  # yEnc1-formal1.txt maximum is 2^62 - 1 (19 digits)
    NAME_CHARS = 1000  # yenc-draft.1.3.txt (2002) maximum is 256 characters
    
    KEYWORD_LINE = re.compile(br"=[\r\n]*y")
    HALF_ESCAPE = re.compile(br"=[\r\n]*$")

try:
    from yenc import Decoder as StreamDecoder
except ImportError:
    from binascii import crc32
    
    class StreamDecoder:
        def __init__(self, file):
            self._file = file
            self._crc = 0
            self._pipe = PipeWriter()
            self._cleanup = ExitStack()
            coroutine = self._pipe.coroutine(self._receive())
            self._cleanup.enter_context(coroutine)
        
        def close(self):
            self._pipe.close()
            del self._pipe
            self._cleanup.close()
        
        def feed(self, data):
            self._pipe.write(data)
        
        def _receive(self):
            while True:
                data = self._pipe.buffer
                pos = data.find(b"=")
                if pos >= 0:
                    data = data[:pos]
                data = data.replace(b"\r", b"").replace(b"\n", b"")
                data = data.translate(self.TABLE)
                # TODO: check data size overflow
                self._crc = crc32(data, self._crc)
                self._file.write(data)
                if pos >= 0:  # Escape character (equals sign)
                    self._pipe.buffer = self._pipe.buffer[pos + 1:]
                    while True:
                        byte = yield from self._pipe.read_one()
                        if byte not in b"\r\n":
                            break
                    # TODO: check for size overflow
                    [byte] = byte
                    data = bytes(((byte - 64 - 42) & bitmask(8),))
                    self._crc = crc32(data, self._crc)
                    self._file.write(data)
                else:
                    try:
                        self._pipe.buffer = yield
                    except EOFError:
                        break
        
        def flush(self):
            pass
        
        def getCrc32(self):
            return format(self._crc, "08x")
        
        TABLE = bytes(range(256))
        TABLE = TABLE[-42:] + TABLE[:-42]

class UnclosingWriter(BufferedIOBase):
    def __init__(self, writer):
        self.writer = writer
    def write(self, *pos, **kw):
        return self.writer.write(*pos, **kw)
