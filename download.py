from shorthand import bitmask
import os, os.path
from shorthand import chunks
import struct
from misc import Context
from io import SEEK_CUR
from log import Progress

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
                Progress.close(self.log)
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
