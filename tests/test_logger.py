from moto import mock_s3
import boto3
from logger import S3StreamLogger

def _setup_module():
    global s3
    global bucket_name
    global s3_client
    global bucket
    global prefix

    s3 = boto3.resource('s3')
    bucket_name = 'test-bucket'
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=bucket_name)
    bucket = s3.Bucket(bucket_name) 
    prefix = 'some/prefix'

class TestS3StreamLoggerLog:

    @mock_s3
    def test_log_multiple_files(self):
        _setup_module()
        logger = S3StreamLogger(bucket_name, prefix, lines=3)
        logger.log('test\ntest\ntest\ntest')
        logger.close()
        objs = list(bucket.objects.filter(Prefix=prefix))
        assert len(objs) == 2, 'Should be two objects'

    @mock_s3
    def test_log_single_file(self):
        _setup_module()
        logger = S3StreamLogger(bucket_name, prefix, lines=10)
        logger.log('test\ntest\ntest\ntest')
        logger.close()
        objs = list(bucket.objects.filter(Prefix=prefix))
        assert len(objs) == 1, 'Should be one object'

    @mock_s3
    def test_log_no_file_without_close(self):
        _setup_module()
        logger = S3StreamLogger(bucket_name, prefix, lines=10)
        logger.log('test\ntest\ntest\ntest')
        objs = list(bucket.objects.filter(Prefix=prefix))
        assert len(objs) == 0, 'Should be no objects'

