# -*- coding: utf-8 -*-

import os
import urllib3
import threading
import manifest
import logging as logging_
logging = logging_.getLogger(__name__)
import zlib
import time
try:
    from cStringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO

import hashlib
import shutil
try:
    from Queue import Queue
except ImportError:
    from queue import Queue

mutex = threading.Lock()


def download_file_by_urls(
    http,
    key,
    urls,
    download_dir,
    output_file,
    transfer_stats_callback=None,
    transfer_stats_callback_kwargs={},
    ):
    _http = http[0]
    filepath_hash = hashlib.sha1(output_file.encode('utf8')).hexdigest()
    temp_file_dir = os.path.join(download_dir, filepath_hash[:2])
    temp_output_filename = os.path.join(temp_file_dir, filepath_hash
            + '.download')
    if os.path.exists(temp_output_filename):
        manifest.digest_path(temp_output_filename, download_dir,
                             compress_level=0)
    with mutex:
        if not os.path.exists(temp_file_dir):
            os.makedirs(temp_file_dir)
    with open(temp_output_filename, 'wb', 8192) as out_file:
        for url in urls:
            start_time = time.time()
            hash = url.split('/')[-1]
            output_dir = os.path.join(download_dir, hash[:2])
            tmp_file = os.path.join(output_dir, hash)
            with mutex:
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
            try:
                if os.path.exists(tmp_file):
                    logging.debug('File already downloaded: '
                                  + tmp_file)
                    data = open(tmp_file, 'rb', 8192).read()
                    net_transfer_bytes = len(data)
                    data = zlib.decompress(data)
                    data_hash = manifest.hash_chunk(data)
                    disk_transfer_bytes = len(data)
                    if hash == data_hash:
                        if transfer_stats_callback \
                            and transfer_stats_callback(key,
                                disk_transfer_bytes, 0,
                                local_transfer=1,
                                **transfer_stats_callback_kwargs) \
                            is False:
                            logging.info('Download aborted for file: '
                                    + output_file, exc_info=1)
                            return
                        out_file.write(data)
                        continue
            except Exception as e:
                logging.exception(str(e))

            retries = 60
            while retries:
                try:
                    logging.debug('GET ' + url)
                    r = _http.request('GET', url, preload_content=False)
                    data_buf = StringIO()
                    for buf in r.stream(32768):
                        data_buf.write(buf)
                        net_transfer_bytes = len(buf)
                        if transfer_stats_callback \
                            and transfer_stats_callback(key, 0,
                                net_transfer_bytes, local_transfer=0,
                                **transfer_stats_callback_kwargs) \
                            is False:
                            logging.info('Download aborted for file: '
                                    + output_file, exc_info=1)
                            return

                    data = data_buf.getvalue()
                    data_buf.close()
                    data = zlib.decompress(data)
                    data_hash = manifest.hash_chunk(data)
                    if data_hash != hash:
                        raise Exception('Error: file corrupted after downloading: '
                                 + url)
                    out_file.write(data)
                    disk_transfer_bytes = len(data)
                    if transfer_stats_callback is not None:
                        if transfer_stats_callback(key,
                                disk_transfer_bytes,
                                net_transfer_bytes=0, local_transfer=0,
                                **transfer_stats_callback_kwargs) \
                            is False:
                            logging.info('Download aborted for file: '
                                    + output_file, exc_info=1)
                            return
                    break
                except Exception as e:
                    logging.error('Error downloading ' + url
                                  + ' for file ' + output_file
                                  + ' (retries remaining '
                                  + str(retries) + ') - ' + str(e),
                                  exc_info=1)
                    sleep_time = 60 - retries + 1
                    time.sleep(sleep_time)
                    _http = urllib3.PoolManager(maxsize=8, block=True,
                            timeout=30)
                    http[0] = _http
                    retries -= 1

            if retries == 0:
                raise Exception('Error: unable to download after 60 retries: '
                                 + url)

    shutil.move(temp_output_filename, output_file)


class NotFinished(Exception):

    pass


class TimeoutQueue(Queue):

    def join_with_timeout(self, timeout):
        self.all_tasks_done.acquire()
        try:
            endtime = time.time() + timeout
            while self.unfinished_tasks:
                remaining = endtime - time.time()
                if remaining <= 0.0:
                    raise NotFinished
                self.all_tasks_done.wait(remaining)
        finally:

            self.all_tasks_done.release()


class HTTPDownloadQueue:

    http = None
    queue = None
    workers = []
    callbacks = {}
    running = True

    def __init__(self):
        self.queue = TimeoutQueue()
        self.http = [urllib3.PoolManager(maxsize=8, block=True,
                     timeout=30)]
        for i in range(8):
            t = threading.Thread(target=self.worker, args=())
            t.daemon = True
            t.start()
            self.workers.append(t)

    def worker(self):
        while self.running:
            work = None
            try:
                work = self.queue.get(False)
            except:
                pass

            if work:
                (
                    key,
                    func,
                    on_complete,
                    on_error,
                    args,
                    kwargs,
                    ) = work
                try:
                    result = func(*args, **kwargs)
                    if on_complete is not None:
                        on_complete(key, result)
                except Exception as e:
                    logging.exception(str(e))
                    if on_error is not None:
                        on_error(key, e)

                try:
                    self.queue.task_done()
                except:
                    pass
            else:

                time.sleep(0.1)

    def download(
        self,
        key,
        urls,
        download_dir,
        output_file,
        on_complete=None,
        on_error=None,
        on_stats=None,
        on_stats_kwargs={},
        ):
        work = (
            key,
            download_file_by_urls,
            on_complete,
            on_error,
            (
                self.http,
                key,
                urls,
                download_dir,
                output_file,
                on_stats,
                on_stats_kwargs,
                ),
            {},
            )
        self.queue.put(work)

    def stop(self):
        logging.debug('Stopping download queue')
        self.running = False

    def join(self, timeout):
        self.queue.join_with_timeout(timeout)

    def __del__(self):
        self.running = False



