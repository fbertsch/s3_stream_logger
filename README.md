#S3 Stream Logger
Sometimes you just want to upload a never ending log to s3. Well, with this utility, just do the following:

```
logger = S3StreamLogger(bucket_name, prefix, lines=1000)
logger.log('some text')
```

And every 1000 lines, a new object will be created with additional data.
