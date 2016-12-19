import smart_open
from datetime import datetime, timedelta

def _clean_s3_path(path):
    if path.endswith('/'):
        return path[:-1]
    return path

class S3StreamLogger:

    _buffer_size = 5 * 1024 ** 2

    def __init__(self, bucket, prefix, bytes=None, lines=None, delta=None, hours=None):
        assert lines or hours, "Must specify a condition of when to write to a new object"
        self.bucket = _clean_s3_path(bucket)
        self.prefix = _clean_s3_path(prefix)
        self.lines = lines
        self.hours = hours
        self._reset()
        if bytes:
            raise NotImplementedError('Writing out bytes is not yet implemented')
        if delta:
            raise NotImplementedError('Writing out after a time delta is not yet implemented')

    def _reset(self):
        self.close()
        self.file = self._next_file()
        self._lines_written = 0

    def _next_file(self):
        self.current_file_time = datetime.now()
        filename = 's3://{}/{}/{}'.format(self.bucket, self.prefix, self.current_file_time.isoformat())
        return smart_open.smart_open(filename, 'w', min_part_size=S3StreamLogger._buffer_size)

    def _append(self, line):
        self.file.write(line + '\n')
        self._lines_written += 1

    def _reset_conditions_satisifed(self, current_time):
        return (self.lines and self._lines_written >= self.lines) or \
               (self.hours and current_time > (self.current_file_time + timedelta(hours=self.hours)))

    def log(self, content):
        lines = [l for l in content.split('\n') if l]
        current_time = datetime.now()
        for line in lines:
            if self._reset_conditions_satisifed(current_time):
                self._reset()
            self._append(line)

    def close(self):
        if hasattr(self, 'file') and self.file:
            self.file.close()
