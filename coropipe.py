"""Pipe with subroutine writer and coroutine reader interfaces"""

from io import BufferedIOBase, BytesIO
from contextlib import contextmanager
from coroutines.results import call_result, RaiseResult

class PipeWriter(BufferedIOBase):
    def writable(self):
        return True
    
    def __init__(self):
        self.coroutine_result = None
        self.buffer = bytes()
        return BufferedIOBase.__init__(self)
    
    def write(self, b):
        if self.coroutine_result:
            raise BrokenPipeError()
        result = call_result(self.coroutine.send, b)
        if isinstance(result, RaiseResult):
            self.coroutine_result = result.generator_result()
        return len(b)
    
    def close(self):
        if not self.coroutine_result:
            result = call_result(self.coroutine.throw, EOFError, EOFError())
            if isinstance(result, RaiseResult):
                self.coroutine_result = result.generator_result()
        return BufferedIOBase.close(self)
    
    @contextmanager
    def coroutine(self, coroutine):
        self.coroutine = coroutine
        result = call_result(next, self.coroutine)
        if isinstance(result, RaiseResult):
            self.coroutine_result = result.generator_result()
        try:
            yield
            if not self.coroutine_result:
                err = RuntimeError("Coroutine did not terminate")
                self.coroutine.throw(RuntimeError, err)
        finally:
            if self.coroutine_result:
                self.coroutine_result.result()
            else:
                coroutine.close()
    
    def read_one(self):
        while not self.buffer:
            self.buffer = yield
        data = self.buffer[:1]
        self.buffer = self.buffer[1:]
        return data
    
    def read_delimited(self, delimiters, size):
        scope = size
        field = BytesIO()
        while True:
            found = scope + 1
            for d in delimiters:
                try:
                    found = self.buffer.find(d, None, found)
                except ValueError:
                    pass
            if found <= scope:
                break
            if len(self.buffer) > scope:
                raise OverflowError("Field length > {!r}".format(size))
            scope -= field.write(self.buffer)
            self.buffer = yield
        field.write(self.buffer[:found])
        self.buffer = self.buffer[found + 1:]
        return (field.getvalue(), self.buffer[found:found + 1])
    
    def read_until_string(self, delimiter, size):  # Unused
        remaining = size
        field = BytesIO()
        while True:
            if len(self.buffer) < len(delimiter):
                self.buffer += yield
            end = min(remaining, max(len(self.buffer) - len(delimiter), 0))
            pos = self.buffer.find(delimiter, None, end + len(delimiter))
            if pos >= 0:
                break
            remaining -= end
            if not remaining:
                raise OverflowError("Field length > {!r}".format(size))
            field.write(self.buffer[:end])
            self.buffer = self.buffer[end:]
        field.write(self.buffer[:pos])
        self.buffer = self.buffer[pos + len(delimiter):]
        return field.getvalue()
    
    def expect(self, expected):
        if not (yield from self.consume_match(expected)):
            raise ValueError("Expected {!r}".format(expected))
    
    def consume_match(self, match):
        while len(self.buffer) < len(match):
            self.buffer += yield
        matched = self.buffer.startswith(match)
        if matched:
            self.buffer = self.buffer[len(match):]
        return matched

if __name__ == "__main__":
    from sys import stdin
    from shutil import copyfileobj
    
    def read(pipe):
        try:
        #limit = 1000
        #buffered = BufferedReader(reader)
            while True:
                item = yield
                if not item.endswith(b"\n"):
                    raise ValueError(repr(item))
                if item == b"\n":
                    return
                print(repr(item))
            #line = yield from buffered.readline(limit)
        except EOFError:
            pass
    
    pipe = PipeWriter()
    with pipe.coroutine(read(pipe)), pipe:
        copyfileobj(stdin.buffer.raw, pipe)
