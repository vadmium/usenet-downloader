#! /usr/bin/env python3

import net
from shorthand import bitmask
from sys import stdin, stdout
from xml.etree import ElementTree
from fnmatch import fnmatchcase
import os, os.path
from contextlib import ExitStack, contextmanager, suppress
from shorthand import chunks
from coropipe import PipeWriter
import socket
from functions import attributes
from configparser import ConfigParser
from functions import setitem
from log import TerminalLog
from log import format_size
import nntp
from download import Download
import yencread
from log import Progress
from warnings import warn

"""
TODO:

specify output file name (for single file dl) or directory name
hop nzbs (max hop limit)
"""

NZB = "{http://www.newzbin.com/DTD/2003/nzb}"

@attributes(param_types=dict(debuglevel=int), cli_context=True)
@contextmanager
def main(address=None, server=None, debuglevel=None):
    if address is not None:
        address = net.Url(netloc=address)
        port = address.port
        username = address.username
        password = address.password
    elif server is not None:
        user_config = os.getenv("XDG_CONFIG_HOME")
        if not user_config:
            user_config = os.getenv("APPDATA")
            if user_config is None:
                user_config = os.path.expanduser("~")
                user_config = os.path.join(user_config, ".config")
        dirs = os.getenv("XDG_CONFIG_DIRS")
        dirs = (dirs or "/etc/xdg").split(os.pathsep)
        
        files = [os.path.join(user_config, "nntp.ini")]
        files.extend(os.path.join(d, "nntp.ini") for d in dirs)
        
        config = ConfigParser(interpolation=None)
        config.read(files)
        server = config["server_" + server]
        address = net.Url(netloc=server["host"])
        port = server.get("port") or address.port
        username = server.get("username") or address.username
        password = server.get("password") or address.password
    
    main = Main()
    with ExitStack() as cleanup:
        if address is None:
            main.nntp = None
        else:
            try:
                # TODO: configurable timeout
                main.nntp = nntp.Client(main.log,
                    address.hostname, port, username, password,
                    debuglevel=debuglevel, timeout=60)
                main.nntp = cleanup.enter_context(main.nntp)
            except (nntp.NNTPPermanentError,) + timeouts as err:
                raise SystemExit(err)
        yield main

@setitem(vars(main), "subcommand_class")
class Main:
    def __init__(self):
        self.log = TerminalLog()
    
    def body(self, id):
        try:
            self.nntp.body(id, file=stdout.buffer)
        except nntp.failure_responses as err:
            raise SystemExit(err)
    
    def decode(self, id):
        pipe = PipeWriter()
        with pipe.coroutine(id_receive(self.log, pipe)), \
        suppress(BrokenPipeError):
            try:
                self.nntp.body(id, file=pipe)
            except nntp.failure_responses as err:
                raise SystemExit(err)
            # EOF error: kill receiver and retry a few times
    
    def over(self, group, first=None, last=None):
        if first is None:
            message_spec = None
        elif last is None:
            message_spec = first
        elif last:
            message_spec = (int(first), int(last))
        else:
            message_spec = (int(first), None)
        self.log.write("{}\n".format(self.nntp.group(group)[0]))
        self.nntp.over(message_spec, file=stdout.buffer)
    
    def hdr(self, group, *pos, **kw):
        self.log.write("{}\n".format(self.nntp.group(group)[0]))
        self.nntp.hdr(*pos, file=stdout.buffer, **kw)
    
    def nzb(self, *include):
        [release, files, rars, pars] = parse_nzb(stdin.buffer, include)
        
        session = dict(log=self.log) # TODO: class
        if self.nntp:
            session["nntp"] = self.nntp
        
        [session["files"], session["size"]] = count_files(files, rars)
        size = format_size(session["size"])
        if release:
            prefix = release + " /"
        else:
            prefix = "Total files:"
        self.log.write("{} {}, ~{}\n".format(prefix, session["files"], size))
        
        if release:
            with suppress(FileExistsError):
                os.mkdir(release)
        
        session["file"] = 0
        for file in files:
            if file.rar is None:
                if self.nntp:
                    session["file"] += 1
                    file.transfer(session)
                    continue
                if file.par is not None:
                    par = pars[file.par]
                    if par is None:
                        continue
                    size = format_size(sum(f.bytes for f in par["files"]))
                    self.log.write("{}[.vol*{:+}].par2: ~{}\n".format(
                        file.par, par["count"], size))
                    pars[file.par] = None
                    continue
                size = format_size(file.bytes)
                self.log.write("{}: ~{}\n".format(file.name, size))
            else:
                volumes = rars[file.rar]
                if not volumes:
                    continue
                if self.nntp:
                    for i in range(len(volumes)):
                        session["file"] += 1
                        volumes[i].transfer(session)
                else:
                    size = sum(v.bytes for v in volumes.values())
                    if len(volumes) > 1:
                        ext = "rar-r{:02}".format(len(volumes) - 2)
                    else:
                        ext = "rar"
                    size = format_size(size)
                    self.log.write("{}.{}: ~{}\n".format(file.rar, ext, size))
                rars[file.rar] = None

def id_receive(log, pipe):
    while True:
        stripped = pipe.buffer.lstrip(b"\r\n")
        if stripped:
            break
        pipe.buffer = yield
    pipe.buffer = stripped
    
    if (yield from pipe.consume_match(b"begin ")):
        yield from pipe.read_delimited(b" ", 30)
        [name, _] = yield from pipe.read_delimited(b"\n",
            yencread.FileDecoder.NAME_CHARS)
        name = name.rstrip(b"\r").decode("ascii")
        with Download(name) as download:
            if download.is_complete(log):
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
                [line, _] = yield from pipe.read_delimited(b"\n",
                    length + 10)
                download.file.write(a2b_uu(start + line[:length]))
            yield from pipe.expect(b"end"[1:])
            [eol, _] = yield from pipe.read_delimited(b"\n", 10)
            if eol.strip():
                raise ValueError(eol)
        return
    decoder = yencread.FileDecoder(log, pipe)
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
    
    with Download(header["name"]) as download:
        if download.is_complete(log):
            return
        # TODO: handle omitted size; if given, make sure it is not ridiculously high
        control = download.open(header["size"], header["size"])
        if not control.is_done(0):
            try:
                yield from decoder.decode_part(download.file, header)
                control.set_done(0)
            finally:
                Progress.close(log)

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
            warn('Expected "yEnc" tag within {!r}'.format(yenc))
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
        with Download(path) as download:
            if download.is_complete(session["log"]):
                return
            
            bytes = format_size(self.bytes)
            session["log"].write("{} ({}/{}, ~{})\n".format(
                self.name, session["file"], session["files"], bytes))
            
            with ExitStack() as cleanup:
                decoder = yencread.FileDecoder(session["log"], PipeWriter())
                coroutine = self._receive(download, decoder)
                cleanup.enter_context(decoder.pipe.coroutine(coroutine))
                cleanup.callback(Progress.close, session["log"])
                for [segment, id] in self.iter_segments():
                    number = int(segment.get("number")) - 1
                    if download.is_done(number):
                        continue
                    for _ in range(5):
                        try:
                            session["nntp"].body(id, file=decoder.pipe)
                        except nntp.failure_responses as err:
                            raise SystemExit(err)
                        except EOFError as err:
                            msg = format(err) or "Connection dropped"
                        except timeouts as err:
                            msg = format(err)
                        else:
                            break
                        session["log"].write(msg + "\n")
                        session["nntp"].log_time()
                        decoder.pipe.close()
                        
                        # Decoder coroutine raises EOFError if input aborted
                        with suppress(EOFError):
                            cleanup.close()
                        
                        session["nntp"].connect()
                        pipe = PipeWriter()
                        decoder = yencread.FileDecoder(session["log"], pipe)
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
        
        control = download.open(header["size"], chunking)
        yield from decoder.decode_part(download.file, header)
        # TODO: base on size if no total; empty dict if neither
        control.set_done(header["part"])
        
        while True:
            header = yield from decoder.parse_header()
            if not header:
                return
            if header["size"] != control.total_length:
                raise ValueError(header["size"])
            if header["name"] != self.name:
                raise ValueError(header["name"])
            # TODO: validate header more; use download.piece_length
            if control.is_done(header["part"]):
                continue
            yield from decoder.decode_part(download.file, header)
            control.set_done(header["part"])
    
    def iter_segments(self):
        for element in self.nzb.iterfind(NZB + "segments"):
            for segment in element:
                if segment.tag != NZB + "segment":
                    raise ValueError(segment.tag)
                [id] = segment.itertext()
                yield (segment, id)

timeouts = (socket.timeout, TimeoutError)

if __name__ == "__main__":
    import clifunc
    clifunc.run()
