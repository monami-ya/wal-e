import collections
import gevent
import socket
import traceback
from urlparse import urlparse

import botocore

from . import calling_format
from wal_e import files
from wal_e import log_help
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE
from wal_e.retries import retry, retry_with_count

logger = log_help.WalELogger(__name__)

_Key = collections.namedtuple('_Key', ['size'])


def _uri_to_bucket(creds, uri, conn=None):
    assert uri.startswith('s3://')
    url_tup = urlparse(uri)
    bucket_name = url_tup.netloc
    conn = calling_format.CallingInfo().connect(creds)
    return conn.Bucket(bucket_name)


def _uri_to_key(creds, uri, conn=None):
    bucket = _uri_to_bucket(creds, uri, conn)
    url_tup = urlparse(uri)
    return bucket.Object(url_tup.path.lstrip('/'))


def uri_put_file(creds, uri, fp, content_encoding=None):
    # Per Boto 2.2.2, which will only read from the current file
    # position to the end.  This manifests as successfully uploaded
    # *empty* keys in S3 instead of the intended data because of how
    # tempfiles are used (create, fill, submit to boto).
    #
    # It is presumed it is the caller's responsibility to rewind the
    # file position, and since the whole program was written with this
    # in mind, assert it as a precondition for using this procedure.
    assert fp.tell() == 0

    bucket = _uri_to_bucket(creds, uri)

    url_tup = urlparse(uri)

    k = bucket.put_object(Key=url_tup.path.lstrip('/'), Body=fp)
    return _Key(size=k.content_length)


def uri_get_file(creds, uri, conn=None):
    k = _uri_to_key(creds, uri, conn=conn)
    # boto3 returns a file descriptor, so we need to read it off the socket
    return k.get()['Body'].read()


def do_lzop_get(creds, url, path, decrypt, do_retry=True):
    """
    Get and decompress a S3 URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert url.endswith('.lzo'), 'Expect an lzop-compressed file'

    def log_wal_fetch_failures_on_error(exc_tup, exc_processor_cxt):
        def standard_detail_message(prefix=''):
            return (prefix + '  There have been {n} attempts to fetch wal '
                    'file {url} so far.'.format(n=exc_processor_cxt, url=url))
        typ, value, tb = exc_tup
        del exc_tup

        # Screen for certain kinds of known-errors to retry from
        if issubclass(typ, socket.error):
            socketmsg = value[1] if isinstance(value, tuple) else value

            logger.info(
                msg='Retrying fetch because of a socket error',
                detail=standard_detail_message(
                    "The socket error's message is '{0}'.".format(socketmsg)))
        else:
            # For all otherwise untreated exceptions, report them as a
            # warning and retry anyway -- all exceptions that can be
            # justified should be treated and have error messages
            # listed.
            logger.warning(
                msg='retrying WAL file fetch from unexpected exception',
                detail=standard_detail_message(
                    'The exception type is {etype} and its value is '
                    '{evalue} and its traceback is {etraceback}'
                    .format(etype=typ, evalue=value,
                            etraceback=''.join(traceback.format_tb(tb)))))

        # Help Python GC by resolving possible cycles
        del tb

    def download():
        with files.DeleteOnError(path) as decomp_out:
            key = _uri_to_key(creds, url)
            with get_download_pipeline(PIPE, decomp_out.f, decrypt) as pl:
                g = gevent.spawn(write_and_return_error, key, pl.stdin)

                try:
                    # Raise any exceptions from write_and_return_error
                    exc = g.get()
                    if exc is not None:
                        raise exc
                except botocore.exceptions.ClientError, e:
                    if e.response['Error']['Code'] == 'NoSuchKey':
                        # Do not retry if the key not present, this
                        # can happen under normal situations.
                        pl.abort()
                        logger.warning(
                            msg=('could no longer locate object while '
                                 'performing wal restore'),
                            detail=('The absolute URI that could not be '
                                    'located is {url}.'.format(url=url)),
                            hint=('This can be normal when Postgres is trying '
                                  'to detect what timelines are available '
                                  'during restoration.'))
                        decomp_out.remove_regardless = True
                        return False
                    else:
                        raise

            logger.info(
                msg='completed download and decompression',
                detail='Downloaded and decompressed "{url}" to "{path}"'
                .format(url=url, path=path))
        return True

    if do_retry:
        download = retry(
            retry_with_count(log_wal_fetch_failures_on_error))(download)

    return download()


def write_and_return_error(key, stream):
    try:
        stream.write(key.get()['Body'].read())
        stream.flush()
    except Exception, e:
        return e
    finally:
        stream.close()
