from shorthand import bitmask
import os, os.path
from shorthand import chunks
import struct
from misc import Context
from io import SEEK_CUR

"""
TODO:

os compat file names
preallocate disk space
posix_fallocate

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

class ControlFile(Context):
    cli_context = True
    
    def __init__(self, path, mode="r"):
        self.file = open(path, mode + "b")
        try:
            [version] = struct.unpack("!H", self.file.read(2))
            if version != 1:
                raise ValueError(version)
            self.file.seek(+4, SEEK_CUR)
            [info_hash_length] = struct.unpack("!L", self.file.read(4))
            self.file.seek(+info_hash_length, SEEK_CUR)
            [self.piece_length, self.total_length] = struct.unpack(
                "!LQ", self.file.read(4 + 8))
            self.file.seek(+8, SEEK_CUR)
            [bitfield_length] = struct.unpack("!L", self.file.read(4))
            minimum = chunks(self.total_length, self.piece_length * 8)
            if bitfield_length < minimum:
                raise ValueError(bitfield_length, minimum)
            self.bitfield = self.file.tell()
        except:
            self.file.close()
            raise
    
    @classmethod
    def create(cls, path, total_length, piece_length):
        self = cls.__new__(cls)
        self.total_length = total_length
        self.piece_length = piece_length
        
        self.file = open(path, "x+b")
        VERSION = 1
        EXTENSION = 0
        INFO_HASH_LENGTH = 0
        self.file.write(struct.pack("!HLL",
            VERSION, EXTENSION, INFO_HASH_LENGTH))
        
        UPLOAD_LENGTH = 0
        bitfield_length = chunks(self.total_length, self.piece_length * 8)
        self.file.write(struct.pack("!LQQL",
            self.piece_length, self.total_length, UPLOAD_LENGTH,
            bitfield_length,
        ))
        self.bitfield = self.file.tell()
        self.file.write(bytes(bitfield_length))
        
        NUM_INFLIGHT_PIECE = 0
        self.file.write(struct.pack("!L", NUM_INFLIGHT_PIECE))
        return self
    
    def close(self):
        self.file.close()
    
    def all_done(self):
        self.file.seek(self.bitfield)
        pieces = chunks(self.total_length, self.piece_length)
        [bytes, bits] = divmod(pieces, 8)
        bitfield = self.file.read(bytes)
        if not all(byte == bitmask(8) for byte in bitfield):
            return False
        if bits:
            [bitfield] = self.file.read(1)
            if bitfield | bitmask(8 - bits) != bitmask(8):
                return False
        return True
    
    def set_done(self, index):
        [offset, bit] = divmod(index, 8)
        offset += self.bitfield
        self.file.seek(offset)
        [bits] = self.file.read(1)
        self.file.seek(offset)
        self.file.write(bytes((bits | 1 << (8 - 1 - bit),)))
    
    def is_done(self, index):
        if not self.file:
            return False
        [offset, bit] = divmod(index, 8)
        self.file.seek(self.bitfield + offset)
        [bits] = self.file.read(1)
        return bits >> (8 - 1 - bit) & 1
    
    def __repr__(self):
        if self.file.closed:
            return "<{} closed>".format(type(self).__name__)
        done = list()
        self.file.seek(self.bitfield)
        pieces = chunks(self.total_length, self.piece_length)
        index = 0
        while True:
            pieces -= 8
            if pieces < 0:
                break
            [bits] = self.file.read(1)
            done.append(format(bits, "08b"))
        if pieces + 8:
            [bits] = self.file.read(1)
            done.append("{:0{}b}".format(bits >> -pieces, pieces + 8))
        repr = "<{} total length {!r}, piece length {!r}, done {}>"
        return repr.format(type(self).__name__,
            self.total_length, self.piece_length, " ".join(done))

class Download(Context):
    def __init__(self, path):
        # TODO: form OS path and/or validate it
        self.path = path
        
        self.control_path = self.path + ".aria2"
        try:
            # TODO: treat truncated file as for new file
            self.control = ControlFile(self.control_path, "r+")
        except FileNotFoundError:
            self.control = None
            
            # If data file exists without control file, assume complete
            self.complete = os.path.exists(self.path)
        else:
            self.complete = False
        self.file = None
    
    def is_complete(self, log):
        if self.complete:
            log.write("{}: assuming complete\n".format(self.path))
        elif self.control:
            log.write("{}: resuming\n".format(self.path))
        return self.complete
    
    def open(self, total_length, piece_length):
        if self.control:
            if total_length != self.control.total_length:
                raise ValueError(self.control.total_length)
            if piece_length != self.control.piece_length:
                raise ValueError(self.control.piece_length)
            
            # TODO: drop any in-flight piece records
            
            mode = "r+b"
        else:
            self.control = ControlFile.create(self.control_path,
                total_length, piece_length)
            
            mode = "xb"
        self.file = open(self.path, mode)
        # TODO: Allocate space if appropriate
        return self.control
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self.file:
            self.file.close()
        if self.control:
            try:
                if not exc_value and not self.control.all_done():
                    raise ValueError(repr(self.control))
            finally:
                self.control.close()
            if not exc_value:
                os.remove(self.control_path)
    
    def is_done(self, index):
        return self.control and self.control.is_done(index)
