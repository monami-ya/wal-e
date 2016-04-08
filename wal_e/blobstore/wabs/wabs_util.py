import collections
import gevent
import socket
import traceback

from azure.common import AzureMissingResourceHttpError
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings

from . import calling_format
from urlparse import urlparse
from wal_e import log_help
from wal_e import files
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE
from wal_e.retries import retry, retry_with_count

assert calling_format

logger = log_help.WalELogger(__name__)

_Key = collections.namedtuple('_Key', ['size'])
WABS_CHUNK_SIZE = 4 * 1024 * 1024


def uri_put_file(creds, uri, fp, content_encoding=None):
    assert fp.tell() == 0
    assert uri.startswith('wabs://')
    url_tup = urlparse(uri)
    fp.seek(0, 2)
    size = fp.tell()

    fp.seek(0, 0)
    conn = BlockBlobService(account_name=creds.account_name,
                account_key=creds.account_key)
    if content_encoding is not None:
        conn.create_blob_from_stream(url_tup.netloc, url_tup.path, fp,
            content_settings=ContentSettings(content_type=content_encoding))
    else:
        conn.create_blob_from_stream(url_tup.netloc, url_tup.path, fp)

    # To maintain consistency with the S3 version of this function we must
    # return an object with a certain set of attributes.  Currently, that set
    # of attributes consists of only 'size'
    return _Key(size=size)


def uri_get_file(creds, uri, conn=None):
    assert uri.startswith('wabs://')
    url_tup = urlparse(uri)

    if conn is None:
        conn = BlockBlobService(account_name=creds.account_name,
                account_key=creds.account_key)
    return conn.get_blob_to_bytes(url_tup.netloc, url_tup.path).content


def do_lzop_get(creds, url, path, decrypt, do_retry=True):
    """
    Get and decompress a S3 URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert url.endswith('.lzo'), 'Expect an lzop-compressed file'
    assert url.startswith('wabs://')

    conn = BlockBlobService(account_name=creds.account_name,
                account_key=creds.account_key)

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
                    "The socket error's message is '{0}'."
                    .format(socketmsg)))
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
            with get_download_pipeline(PIPE, decomp_out.f, decrypt) as pl:
                g = gevent.spawn(write_and_return_error, url, conn, pl.stdin)

                try:
                    # Raise any exceptions guarded by
                    # write_and_return_error.
                    exc = g.get()
                    if exc is not None:
                        raise exc
                except AzureMissingResourceHttpError:
                    # Short circuit any re-try attempts under certain race
                    # conditions.
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

            logger.info(
                msg='completed download and decompression',
                detail='Downloaded and decompressed "{url}" to "{path}"'
                .format(url=url, path=path))
        return True

    if do_retry:
        download = retry(
            retry_with_count(log_wal_fetch_failures_on_error))(download)

    return download()


def write_and_return_error(url, conn, stream):
    try:
        data = uri_get_file(None, url, conn=conn)
        stream.write(data)
        stream.flush()
    except Exception, e:
        return e
    finally:
        stream.close()
