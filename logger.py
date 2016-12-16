import smart_open
from datetime import datetime

def _clean_s3_path(path):
    if path.endswith('/'):
        return path[:-1]
    return path

class S3StreamLogger:

    _buffer_size = 5 * 1024 ** 2

    def __init__(self, bucket, prefix, bytes=None, lines=None, delta=None, daily=False):
        self.bucket = _clean_s3_path(bucket)
        self.prefix = _clean_s3_path(prefix)
        self.lines = lines
        self._reset()
            raise NotImplementedError('Daily writing is not yet implemented')
        if bytes:
            raise NotImplementedError('Writing out bytes is not yet implemented')
        if delta:
            raise NotImplementedError('Writing out after a time delta is not yet implemented')

    def _reset(self):
        self.close()
        self.file = self._next_file()
        self._lines_written = 0

    def _next_file(self):
        filename = 's3://{}/{}/{}'.format(self.bucket, self.prefix, datetime.now().isoformat())
        return smart_open.smart_open(filename, 'w', min_part_size=S3StreamLogger._buffer_size)

    def _append(self, line):
        self.file.write(line + '\n')
        self._lines_written += 1

    def _reset_conditions_satisifed(self):
        return self._lines_written >= self.lines

    def log(self, content):
        lines = [l for l in content.split('\n') if l]
        for line in lines:
            if self._reset_conditions_satisifed():
                self._reset()
            self._append(line)

    def close(self):
        if hasattr(self, 'file') and self.file:
            self.file.close()
