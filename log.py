from sys import stderr
import os
from io import UnsupportedOperation
from warnings import warn
import time

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

class Progress:
    SAMPLES = 30
    # TODO: try keeping samples for say up to 10 s, but drop samples
    # that are older than 10 s rather than having a fixed # of samples
    
    def __init__(self, log, total, progress=0):
        self.log = log
        self.total = total
        self.last = time.monotonic()
        self.samples = [(self.last, progress)] * self.SAMPLES
        self.sample = 0
    
    def update(self, progress):
        now = time.monotonic()
        interval = now - self.last
        if interval < 0.1:
            return
        self.last = now
        [then, prev] = self.samples[self.sample]
        rate = (progress - prev) / (now - then)
        self.samples[self.sample] = (now, progress)
        self.sample = (self.sample + 1) % self.SAMPLES
        # TODO: detect non terminal, including IDLE; allow this determination to be overridden
        if rate:
            eta = (self.total - progress) / rate
        if rate and eta < 9999 * 60 + 59:
            [min, sec] = divmod(eta, 60)
        else:
            min = 9999
            sec = 99
        progress /= self.total
        # TODO: round progress pc down so 100% means exactly done
        # Flexible units for rate
        self.log.carriage_return()
        self.log.clear_eol()
        self.log.write("{:5.1%}{:6.0f}kB/s{:5}m{:02}s".format(
            progress, rate / 1000, -int(min), int(sec)))
        self.log.flush()
    
    @classmethod
    def close(cls, log):
        log.carriage_return()
        log.clear_eol()
