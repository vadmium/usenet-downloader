import net
from nntplib import NNTP, NNTPTemporaryError, NNTPPermanentError, NNTPError
from shorthand import bitmask
from sys import stdin, stderr, stdout
from xml.etree import ElementTree
from fnmatch import fnmatchcase
import os, os.path
from contextlib import ExitStack, contextmanager, closing, suppress
from shorthand import chunks
import struct
import time
from coropipe import PipeWriter
from misc import Context
from io import SEEK_CUR, UnsupportedOperation
import socket
import re
from io import BufferedIOBase
from functions import attributes
from warnings import warn
from configparser import ConfigParser

"""
TODO:

specify output file name (for single file dl) or directory name
os compat file names
preallocate disk space
posix_fallocate
hop nzbs (max hop limit)
when cancelling body (e.g. file found to be complete), determine whether to
    abort connection (if a lot to dl), or to do a dummy transfer (small amount
    to dl)
option to reconnect after given time, to avoid server inconvenient
    disconnection

sequentially appended file
    * file size indicates completion
    * no truncation needed to resume with basic tool
    * inefficient for transferring chunks concurrently or out of order
seek over holes
pre-allocate space
    * avoids fragmentation
    * early indication if not enough space
    * naive file system zeroing
"""

NZB = "{http://www.newzbin.com/DTD/2003/nzb}"

@attributes(param_types=dict(debuglevel=int))
def main(command, *args, address=None, server=None, debuglevel=None):
    if address is not None:
        address = net.Url(netloc=address)
        port = address.port
        username = address.username
        password = address.password
    elif server is not None:
        config = ConfigParser(interpolation=None)
        with open("nntp.ini") as file:
            config.read_file(file)
        server = config["server_" + server]
        address = net.Url(netloc=server["host"])
        port = server.get("port") or address.port
        username = server.get("username") or address.username
        password = server.get("password") or address.password
    
    log = TerminalLog()
    with ExitStack() as cleanup:
        if address is not None:
            try:
                # TODO: configurable timeout
                nntp = NntpClient(log,
                    address.hostname, port, username, password,
                    debuglevel=debuglevel, timeout=60)
                nntp = cleanup.enter_context(nntp)
            except NNTPPermanentError as err:
                raise SystemExit(err)
        
        if command == "decode":
            return transfer_id(nntp, log, *args)
        if command == "over":
            return over(nntp, log, *args)
        if command == "hdr":
            return hdr(nntp, log, *args)
        if command != "nzb":
            raise SystemExit("Unknown command {!r}".format(command))
        
        [release, files, rars, pars] = parse_nzb(stdin.buffer, args)
        
        session = dict(log=log) # TODO: class
        if address is not None:
            session["nntp"] = nntp
        
        [session["files"], session["size"]] = count_files(files, rars)
        size = format_size(session["size"])
        if release:
            prefix = release + " /"
        else:
            prefix = "Total files:"
        log.write("{} {}, ~{}\n".format(prefix, session["files"], size))
        
        if release:
            with suppress(FileExistsError):
                os.mkdir(release)
        
        session["file"] = 0
        for file in files:
            if file.rar is None:
                if address is not None:
                    session["file"] += 1
                    file.transfer(session)
                    continue
                if file.par is not None:
                    par = pars[file.par]
                    if par is None:
                        continue
                    size = format_size(sum(f.bytes for f in par["files"]))
                    log.write("{}[.vol*{:+}].par2: ~{}\n".format(
                        file.par, par["count"], size))
                    pars[file.par] = None
                    continue
                size = format_size(file.bytes)
                log.write("{}: ~{}\n".format(file.name, size))
            else:
                volumes = rars[file.rar]
                if not volumes:
                    continue
                if address is None:
                    size = sum(v.bytes for v in volumes.values())
                    if len(volumes) > 1:
                        ext = "rar-r{:02}".format(len(volumes) - 1)
                    else:
                        ext = "rar"
                    size = format_size(size)
                    log.write("{}.{}: ~{}\n".format(file.rar, ext, size))
                else:
                    for i in range(len(volumes)):
                        session["file"] += 1
                        volumes[i].transfer(session)
                rars[file.rar] = None

def transfer_id(nntp, log, id):
    pipe = PipeWriter()
    with pipe.coroutine(id_receive(log, pipe)), suppress(BrokenPipeError):
        try:
            nntp.body(id, file=pipe)
        except (NNTPTemporaryError, NNTPPermanentError) as err:
            raise SystemExit(err)
        # EOF error: kill receiver and retry a few times

def id_receive(log, pipe):
    if (yield from pipe.consume_match(b"begin ")):
        yield from pipe.read_delimited(b" ", 30)
        name = yield from pipe.read_delimited(b"\n",
            YencFileDecoder.NAME_CHARS)
        name = name.rstrip(b"\r").decode("ascii")
        with Download(log, name) as download:
            if download.complete:
                return
            if download.control:
                raise SystemExit("Cannot resume UU-encoded download")
            # download.open(unknown-size)
            download.file = open(name, "xb")
            while True:
                start = yield from pipe.read_one()
                if start == b"e":
                    break
                [length] = start
                if length < 32 or length > 96:
                    raise ValueError(length)
                length = chunks(((length - 32) & bitmask(6)) * 8, 6)
                line = yield from pipe.read_delimited(b"\n", length + 10)
                download.file.write(a2b_uu(start + line[:length]))
            yield from pipe.expect(b"nd")
            if (yield from pipe.read_delimited(b"\n", 10)).strip():
                raise ValueError()
        return
    decoder = YencFileDecoder(log, pipe)
    header = yield from decoder.parse_header()
    if header.get("part") or header.get("total", 1) != 1:
        raise ValueError(header)
    if header.get("begin"):
        raise ValueError(header["begin"])
    size = header.get("size")
    end = header.get("end")
    if None not in (size, end) and size != end:
        raise ValueError(header)
    
    log.write("Transferring {}".format(header["name"]))
    if size is not None:
        log.write(", {}".format(format_size(size)))
    log.write("\n")
    
    with Download(log, header["name"]) as download:
        if download.complete:
            return
        # TODO: handle omitted size; if given, make sure it is not ridiculously high
        download.open(header["size"], header["size"])
        if not download.is_done(0):
            yield from decoder.decode_part(download.file, header)
            download.set_done(0)

def over(nntp, log, group, first=None, last=None):
    if first is None:
        message_spec = None
    elif last is None:
        message_spec = first
    elif last:
        message_spec = (int(first), int(last))
    else:
        message_spec = (int(first), None)
    log.write("{}\n".format(nntp.group(group)[0]))
    nntp.over(message_spec, file=stdout.buffer)

def hdr(nntp, log, group, *pos, **kw):
    log.write("{}\n".format(nntp.group(group)[0]))
    nntp.hdr(*pos, file=stdout.buffer, **kw)

def parse_nzb(stream, include=None):
    nzb = ElementTree.parse(stream).getroot()
    if nzb.tag != NZB + "nzb":
        raise ValueError(nzb.tag)
    
    # [(key, file)]: Files in NZB, to be put into a sensible order based
    # on article subject names and with correct rar volume order
    # message-id could be relevant, but the partXofY bit is for parts of a single file
    # put .nfo files before other files with same stem
    # put [.volN+N].par2 files after other files with same stem,
    # plain .par2 first, then .volN* in increasing N order
    # put .sfv files before files with same stem (? order with par2 files)
    # what about .par (no 2) files?
    # as low priority fallback, group by poster, but retain approximate
    # poster order as in NZB
    files = list()
    rars = dict()
    pars = dict()
    unique_release = None
    for file in nzb.iterfind(NZB + "file"):
        file = NzbFile(file)
        if unique_release not in (None, file.release):
            raise ValueError(file.release)
        unique_release = file.release
        
        # TODO: if there is no subject, or for equal subjects:
        # what about sorting by date?
        if not include or any(fnmatchcase(file.name, i) for i in include):
            files.append(file)
        
        [stem, *ext] = file.name.rsplit(".", 1)
        
        i = None
        if ext == ["rar"]:
            i = 0
        if ext and ext[0][0] == "r":
            with suppress(ValueError):
                i = 1 + int(ext[0][1:])
        if i is None:
            file.rar = None
        else:
            file.rar = stem
            rars.setdefault(stem, dict())[i] = file
        
        try:
            if ext != ["par2"]:
                raise ValueError()
            [stem, *vol] = stem.rsplit(".vol", 1)
            if vol:
                [vol] = vol
                [vol, count] = vol.split("+", 1)
                if not vol.isnumeric():
                    raise ValueError()
                count = int(count)
            else:
                count = 0
        except ValueError:
            file.par = None
        else:
            file.par = stem
            par = pars.setdefault(stem, dict(count=0, files=list()))
            par["count"] += count
            par["files"].append(file)
    files.sort(key=file_key)
    return (unique_release, files, rars, pars)

def file_key(file):
    return (file.nzb.get("poster"), file.nzb.get("subject"))

def count_files(files, rars):
    count = 0
    size = 0
    rars_counted = set()
    for file in files:
        if file.rar is None:
            count += 1
            size += file.bytes
        elif file.rar not in rars_counted:
            volumes = rars[file.rar].values()
            count += len(volumes)
            size += sum(v.bytes for v in volumes)
            rars_counted.add(file.rar)
    return (count, size)

class NzbFile:
    """Represents a single file described in an NZB file"""
    
    def __init__(self, nzb):
        self.nzb = nzb
        
        subject = self.nzb.get("subject").rsplit('"', 2)
        [self.release, self.name, yenc] = subject
        if "yEnc" not in yenc:
            raise ValueError(yenc)
        try:
            [self.release, _] = self.release.rsplit(" ]", 1)  # TODO: always jump to 2nd last bracketted box
            [_, self.release] = self.release.rsplit("[ ", 1)  # TODO: skip nested brackets
        except ValueError:
            self.release = ""
        
        self.bytes = 0
        ids = set()
        for [segment, id] in self.iter_segments():
            self.bytes += int(segment.get("bytes"))
            if id in ids:
                raise ValueError(segment)
            ids.add(set)
    
    def transfer(self, session):
        path = os.path.join(self.release, self.name)
        with Download(session["log"], path) as download:
            if download.complete:
                return
            
            bytes = format_size(self.bytes)
            session["log"].write("{} ({}/{}, ~{})\n".format(
                self.name, session["file"], session["files"], bytes))
            
            with ExitStack() as cleanup:
                decoder = YencFileDecoder(session["log"], PipeWriter())
                coroutine = self._receive(download, decoder)
                cleanup.enter_context(decoder.pipe.coroutine(coroutine))
                for [segment, id] in self.iter_segments():
                    number = int(segment.get("number")) - 1
                    if download.is_done(number):
                        continue
                    for _ in range(5):
                        try:
                            session["nntp"].body(id, file=decoder.pipe)
                        except (NNTPTemporaryError, NNTPPermanentError) as \
                        err:
                            raise SystemExit(err)
                        except EOFError as err:
                            msg = format(err) or "Connection dropped"
                        except (socket.timeout, TimeoutError) as err:
                            msg = format(err)
                        else:
                            break
                        session["log"].write(msg + "\n")
                        # TODO: time duration formatter
                        session["log"].write("Connection lasted {:.0f}m\n".format((time.monotonic() - session["nntp"].connect_time)/60))
                        decoder.pipe.close()
                        with suppress(EOFError):
                            cleanup.close()
                        session["nntp"].connect()
                        pipe = PipeWriter()
                        decoder = YencFileDecoder(session["log"], pipe)
                        coroutine = self._receive(download, decoder)
                        # TODO: should not re-open download etc
                        coroutine = decoder.pipe.coroutine(coroutine)
                        cleanup.enter_context(coroutine)
                    else:
                        raise SystemExit("Failed retrieving {} segment {} "
                            "<{}> (attempts: {})".format(
                            self.name, 1 + number, id))
                decoder.pipe.close()
    
    def _receive(self, download, decoder):
        header = yield from decoder.parse_header()
        if not header:
            return
        if header["part"]:
            chunking = header["begin"] // header["part"]
        else:
            chunking = header["end"] - header["begin"]
        decoder.validate_header(header, chunking)
        
        download.open(header["size"], chunking)
        yield from decoder.decode_part(download.file, header)
        # TODO: base on size if no total; empty dict if neither
        download.set_done(header["part"])
        
        while True:
            header = yield from decoder.parse_header()
            if not header:
                return
            if header["size"] != download.total_length:
                raise ValueError(header["size"])
            if header["name"] != self.name:
                raise ValueError(header["name"])
            # TODO: validate header more; use download.piece_length
            if download.is_done(header["part"]):
                continue
            yield from decoder.decode_part(download.file, header)
            download.set_done(header["part"])
    
    def iter_segments(self):
        for element in self.nzb.iterfind(NZB + "segments"):
            for segment in element:
                if segment.tag != NZB + "segment":
                    raise ValueError(segment.tag)
                [id] = segment.itertext()
                yield (segment, id)

class Download(Context):
    def __init__(self, log, path):
        # TODO: form OS path and/or validate it
        self.log = log
        self.path = path
        
        self.control_path = self.path + ".aria2"
        try:
            self.control = open(self.control_path, "r+b")
        except FileNotFoundError:
            self.control = None
            
            # If data file exists without control file, assume complete
            self.complete = os.path.exists(self.path)
            if self.complete:
                self.log.write("{}: assuming complete\n".format(self.path))
        else:
            try:
                self.log.write("{}: resuming\n".format(self.path))
                self.complete = False
                
                # TODO: treat truncated file as for new file
                [version] = struct.unpack("!H", self.control.read(2))
                if version != 1:
                    raise ValueError(version)
                self.control.seek(+4, SEEK_CUR)
                [info_hash_length] = struct.unpack(
                    "!L", self.control.read(4))
                self.control.seek(+info_hash_length, SEEK_CUR)
                [self.piece_length, self.total_length] = struct.unpack(
                    "!LQ", self.control.read(4 + 8))
                self.control.seek(+8, SEEK_CUR)
                [bitfield_length] = struct.unpack("!L", self.control.read(4))
                minimum = chunks(self.total_length, self.piece_length * 8)
                if bitfield_length < minimum:
                    raise ValueError(bitfield_length, minimum)
                self.bitfield = self.control.tell()
            except:
                self.control.close()
                raise
        self.file = None
    
    def open(self, total_length, piece_length):
        if self.control:
            if total_length != self.total_length:
                raise ValueError(self.total_length)
            if piece_length != self.piece_length:
                raise ValueError(self.piece_length)
            
            # TODO: drop any in-flight piece records
            
            mode = "r+b"
        else:
            self.total_length = total_length
            self.piece_length = piece_length
            
            self.control = open(self.control_path, "x+b")
            VERSION = 1
            EXTENSION = 0
            INFO_HASH_LENGTH = 0
            self.control.write(struct.pack("!HLL",
                VERSION, EXTENSION, INFO_HASH_LENGTH))
            
            UPLOAD_LENGTH = 0
            bitfield_length = chunks(self.total_length,
                self.piece_length * 8)
            self.control.write(struct.pack("!LQQL",
                self.piece_length, self.total_length, UPLOAD_LENGTH,
                bitfield_length,
            ))
            self.bitfield = self.control.tell()
            self.control.write(bytes(bitfield_length))
            
            NUM_INFLIGHT_PIECE = 0
            self.control.write(struct.pack("!L", NUM_INFLIGHT_PIECE))
            
            mode = "xb"
        self.file = open(self.path, mode)
        # TODO: Allocate space if appropriate
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self.file:
            self.file.close()
        if self.control:
            try:
                self.log.carriage_return()
                self.log.clear_eol()
                if not exc_value:
                    self.control.seek(self.bitfield)
                    pieces = chunks(self.total_length, self.piece_length)
                    [bytes, bits] = divmod(pieces, 8)
                    bitfield = self.control.read(bytes)
                    if not all(byte == bitmask(8) for byte in bitfield):
                        raise ValueError(bitfield)
                    [bitfield] = self.control.read(1)
                    if bits and bitfield | bitmask(8 - bits) != bitmask(8):
                        raise ValueError(bitfield)
            finally:
                self.control.close()
            if not exc_value:
                os.remove(self.control_path)
    
    def set_done(self, index):
        [offset, bit] = divmod(index, 8)
        offset += self.bitfield
        self.control.seek(offset)
        [bits] = self.control.read(1)
        self.control.seek(offset)
        self.control.write(bytes((bits | 1 << (8 - 1 - bit),)))
    
    def is_done(self, index):
        if not self.control:
            return False
        [offset, bit] = divmod(index, 8)
        self.control.seek(self.bitfield + offset)
        [bits] = self.control.read(1)
        return bits >> (8 - 1 - bit) & 1

class YencFileDecoder:
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
            yield from self.pipe.expect(b"=ybegin part=")
        except EOFError:
            return None
        header["part"] = yield from self.pipe.read_delimited(b" total=",
            self.PART_DIGITS)
        header["part"] = int(header["part"]) - 1
        header["total"] = yield from self.pipe.read_delimited(
            b" line=128 size=", self.PART_DIGITS)
        header["total"] = int(header["total"])
        header["size"] = yield from self.pipe.read_delimited(b" name=",
            self.SIZE_DIGITS)
        header["size"] = int(header["size"])
        
        # Not treating quote characters specially, despite yEnc-Notes3.txt
        # (2002) saying quotes should be removed if included, since other
        # specifications do not mention quotes or are not as clear, and this
        # is simpler. Also, not trimming leading or trailing spaces, since
        # this is simpler, and there doesn't seem to be any evidence that
        # unwanted spaces may be added.
        header["name"] = yield from self.pipe.read_delimited(b"\n",
            self.NAME_CHARS)
        header["name"] = header["name"].rstrip(b"\r").decode("ascii")
        
        if (yield from self.pipe.consume_match(b"=ypart begin=")):
            header["begin"] = yield from self.pipe.read_delimited(b" end=",
                self.SIZE_DIGITS)
            header["begin"] = int(header["begin"]) - 1
            header["end"] = yield from self.pipe.read_delimited(b"\n",
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
        # TODO: limit decoded data to (end - begin), or size of not partial, or some hard-coded limit if no size given
        file.seek(header["begin"])
        # TODO: do not allow =y lines, newlines, etc to exceed data bytes by say 100
        SAMPLES = 30
        # TODO: try keeping samples for say up to 10 s, but drop samples
        # that are older than 10 s rather than having a fixed # of samples
        last = time.monotonic()
        samples = [(last, header["begin"])] * SAMPLES
        sample = 0
        with closing(YencStreamDecoder(UnclosingWriter(file))) as decoder:
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
                    
                    now = time.monotonic()
                    interval = now - last
                    if interval >= 0.1:
                        last = now
                        progress = file.tell()
                        [then, prev] = samples[sample]
                        rate = (progress - prev) / (now - then)
                        samples[sample] = (now, progress)
                        sample = (sample + 1) % SAMPLES
                        # TODO: incorporate into total of all files; update total of all files with real file size
                        # TODO: detect non terminal, including IDLE; allow this determination to be overridden
                        if rate:
                            eta = (header["size"] - progress) / rate
                        if rate and eta < 9999 * 60 + 59:
                            [min, sec] = divmod(eta, 60)
                        else:
                            min = 9999
                            sec = 99
                        progress = progress / header["size"]
                        # TODO: round progress pc down so 100% means exactly done
                        # Flexible units for rate
                        # keep samples over multiple parts
                        self.log.carriage_return()
                        self.log.clear_eol()
                        self.log.write("{:5.1%}{:6.0f}kB/s{:5}m{:02}s".format(
                            progress, rate / 1000, -int(min), int(sec)))
                        self.log.flush()
        if file.tell() != header["end"]:
            raise ValueError(header["end"])
        
        expected = "end size={} part={} pcrc32={}\r\n"
        size = header["end"] - header["begin"]
        crc = decoder.getCrc32()
        expected = expected.format(size, 1 + header["part"], crc)
        yield from self.pipe.expect(expected.encode("ascii"))
        
        # TODO: explicitly detect second yEnc object and report as error,
        # since this is specifically allowed
    
    PART_DIGITS = 10  # yEnc1-formal1.txt (2002) maximum is 999 (3 digits)
    SIZE_DIGITS = 30  # yEnc1-formal1.txt maximum is 2^62 - 1 (19 digits)
    NAME_CHARS = 1000  # yenc-draft.1.3.txt (2002) maximum is 256 characters
    
    KEYWORD_LINE = re.compile(br"=[\r\n]*y")
    HALF_ESCAPE = re.compile(br"=[\r\n]*$")

try:
    from yenc import Decoder as YencStreamDecoder
except ImportError:
    from binascii import crc32
    
    class YencStreamDecoder:
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

class NntpClient(Context):
    def __init__(self, log,
    hostname, port=None, username=None, password=None, *,
    debuglevel=None, **timeout):
        self.log = log
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.debuglevel = debuglevel
        self.timeout = timeout
        Context.__init__(self)
        self.connect()
    
    def connect(self):
        address = net.format_addr((self.hostname, self.port))
        self.log.write("Connecting to {}\n".format(address))
        if self.port is None:
            port = ()
        else:
            port = (self.port,)
        self.connect_time = time.monotonic()
        self.nntp = NNTP(self.hostname, *port, **self.timeout)
        with ExitStack() as cleanup:
            cleanup.push(self)
            if self.debuglevel is not None:
                self.nntp.set_debuglevel(self.debuglevel)
            self.nntp.getwelcome()
            
            if self.username is not None:
                self.log.write("Logging in\n")
                with self.handle_abort():
                    self.nntp.login(self.username, self.password)
            self.log.write("Connected\n")
            cleanup.pop_all()
    
    def body(self, id, *pos, **kw):
        id = "<{}>".format(id)
        retry = 0
        while True:
            try:
                with self.handle_abort():
                    self.nntp.body(id, *pos, **kw)
                break
            except (NNTPTemporaryError, NNTPPermanentError) as err:
                [code, *msg] = err.response.split(maxsplit=1)
                if code == "400":
                    [msg] = msg or (None,)
                    if not msg:
                        msg = "Server shut down connection"
                elif code[1] == "0" and not retry:
                    msg = err.response
                else:
                    raise
            self.log.write(msg + "\n")
            # TODO: time duration formatter
            self.log.write("Connection lasted {:.0f}m\n".format((time.monotonic() - self.connect_time)/60))
            if retry >= 60:
                raise TimeoutError()
            self.close()
            time.sleep(retry)
            if not retry:
                start = time.monotonic()
            self.connect()
            if retry:
                retry *= 2
            else:
                retry = time.monotonic() - start
                if retry <= 0:
                    retry = 0.5
    
    def group(self, *pos, **kw):
        with self.handle_abort():
            return self.nntp.group(*pos, **kw)
    def over(self, *pos, **kw):
        with self.handle_abort():
            return self.nntp.over(*pos, **kw)
    def hdr(self, *pos, **kw):
        with self.handle_abort():
            return self.nntp.xhdr(*pos, **kw)
    
    @contextmanager
    def handle_abort(self):
        try:
            yield
        except (NNTPTemporaryError, NNTPPermanentError):
            raise  # NNTP connection still intact
        except:
            # Protocol is disrupted so abort the connection straight away
            self.close()
            raise
    
    def close(self):
        if not self.nntp:
            return
        
        # Ignore failure of inappropriate QUIT command
        with suppress(NNTPError), self.nntp:
            pass
        self.nntp = None

class TerminalLog:
    def __init__(self):
        # Defaults
        self.tty = False
        self.curses = None
        
        if not stderr:
            return
        try:
            if not stderr.buffer.isatty():
                raise UnsupportedOperation()
        except (AttributeError, UnsupportedOperation):
            return
        
        self.tty = True
        try:
            import curses
        except ImportError:
            return
        self.write(str())
        self.flush()
        termstr = os.getenv("TERM", "")
        fd = stderr.buffer.fileno()
        try:
            curses.setupterm(termstr, fd)
        except curses.error as err:
            warn(err)
        self.curses = curses
    
    def write(self, text):
        if stderr:
            stderr.write(text)
            self.flushed = False
    
    def flush(self):
        if stderr and not self.flushed:
            stderr.flush()
            self.flushed = True
    
    def carriage_return(self):
        if self.curses:
            self.tput("cr")
        elif self.tty:
            self.write("\r")
        else:
            self.write("\n")
    
    def clear_eol(self):
        return self.tput("el")
    
    def tput(self, capname):
        if not self.curses:
            return
        string = self.curses.tigetstr(capname)
        segs = string.split(b"$<")
        string = segs[0]
        string += bytes().join(s.split(b">", 1)[1] for s in segs[1:])
        self.flush()
        stderr.buffer.write(string)

def format_size(size):
    """
    format_size(0) -> "0 B"  # Only instance of leading zero
    format_size(33) -> "33 B"  # No extra significant digits
    format_size(10150) -> "10.2 kB"  # Round half up to even; three digits
    format_size(222500) -> "222 kB"  # Half down to even; no decimal point
    format_size(9876543) -> "9876 kB"  # Four digits
    format_size(9999500) -> "10.0 MB"  # Rounding forces prefix change
    format_size(9999499999) -> "9999 MB"  # Only round off once
    """
    ndigits = 0
    multiple = 1
    prefix = ""
    prefixes = iter("kMGTPEZY")
    while True:
        rounded = round(size, ndigits)
        if rounded < 10000 * multiple:
            break
        p = next(prefixes, None)
        if not p:
            break
        prefix = p
        
        multiple *= 1000
        ndigits -= 2
        rounded = round(size, ndigits)
        if rounded < 100 * multiple:
            [int, fract] = divmod(rounded, multiple)
            return "{}.{} {}B".format(int, fract * 10 // multiple, prefix)
        ndigits -= 1
    return "{} {}B".format(rounded // multiple, prefix)

class UnclosingWriter(BufferedIOBase):
    def __init__(self, writer):
        self.writer = writer
    def write(self, *pos, **kw):
        return self.writer.write(*pos, **kw)

if __name__ == "__main__":
    import clifunc
    clifunc.run()
