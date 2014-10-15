import net
from nntplib import NNTP, NNTPError
from contextlib import ExitStack, contextmanager, suppress
import time
from misc import Context

"""
TODO:
when cancelling body (e.g. file found to be complete), determine whether to
    abort connection (if a lot to dl), or to do a dummy transfer (small amount
    to dl)
option to reconnect after given time, to avoid server inconvenient
    disconnection
"""

# Part of API
from nntplib import NNTPTemporaryError, NNTPPermanentError
failure_responses = (NNTPTemporaryError, NNTPPermanentError)

class Client(Context):
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
            self.log.write("{}\n".format(self.nntp.getwelcome()))
            
            if self.username is not None:
                self.log.write("Logging in as {}\n".format(self.username))
                with self.handle_abort():
                    self.nntp.login(self.username, self.password)
                self.log.write("Logged in\n")
            cleanup.pop_all()
    
    def body(self, id, *pos, **kw):
        id = "<{}>".format(id)
        retry = 0
        while True:
            try:
                with self.handle_abort():
                    self.nntp.body(id, *pos, **kw)
                break
            except failure_responses as err:
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
            self.log_time()
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
        except failure_responses:
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
    
    def log_time(self):
        # TODO: time duration formatter
        self.log.write("Connection lasted {:.0f}m\n".format((time.monotonic() - self.connect_time)/60))
