# -*- coding: utf-8 -*-

from manifest import *
try:
    import urllib2
    from urllib2 import urlopen
except ImportError:
    import urllib as urllib2
    from urllib.request import urlopen

import shutil
import copy
import json
import logging as logging_
logging = logging_.getLogger(__name__)
import downloader


def apply_patch_diff(
    dst_manifest,
    diff,
    src_url_root,
    target_dir,
    download_dir,
    stats_callback=None,
    stats_callback_kwargs={},
    cache_dir=None,
    ):
    download_queue = downloader.HTTPDownloadQueue()
    errors = []
    completed = []
    transfer_stats = {
        'net_bytes_transfered': 0,
        'net_patch_bytes_transfered': 0,
        'disk_bytes_transfered': 0,
        'disk_patch_bytes_transfered': 0,
        'percent_complete': 0,
        'disk_bytes_total': diff['total_size'],
        'disk_patch_bytes_total': diff['patch_size'],
        }
    aborted = [False]
    if cache_dir is None:
        cache_dir = target_dir
    for encoded_path in diff['remove']:
        rel_path = decode_path(encoded_path,
                               dst_manifest['filepath_encoding'])
        full_path = os.path.join(target_dir, rel_path)
        if os.path.isfile(full_path):
            digest_path(full_path, download_dir, 0)

    for encoded_path in diff['update']:

        def _on_download_complete(key, result):
            rel_path = decode_path(key, dst_manifest['filepath_encoding'
                                   ])
            full_path = os.path.join(target_dir, rel_path)
            logging.debug('Download complete:' + rel_path)
            completed.append(rel_path)
            mtime = dst_manifest['files'][key]['mtime']
            os.utime(full_path, (mtime, mtime))

        def _on_download_error(key, e):
            rel_path = decode_path(key, dst_manifest['filepath_encoding'
                                   ])
            errors.append((rel_path, e))

        def _on_stats(
            key,
            disk_transfer_bytes,
            net_transfer_bytes,
            **kwargs
            ):
            rel_path = decode_path(key, dst_manifest['filepath_encoding'
                                   ])
            transfer_stats['net_bytes_transfered'] += net_transfer_bytes
            transfer_stats['disk_bytes_transfered'] += \
                disk_transfer_bytes
            if not transfer_stats.get('local_transfer', 0):
                transfer_stats['disk_patch_bytes_transfered'] += \
                    disk_transfer_bytes
                transfer_stats['net_patch_bytes_transfered'] += \
                    net_transfer_bytes
            transfer_stats['percent_complete'] = 100.0 \
                * transfer_stats['disk_bytes_transfered'] \
                / diff['total_size']
            transfer_stats.update(kwargs)
            if stats_callback is not None:
                result = stats_callback(**transfer_stats)
                if result is False:
                    aborted[0] = True
                    download_queue.stop()
                return result

        def download_file(encoded_path):
            rel_path = decode_path(encoded_path,
                                   dst_manifest['filepath_encoding'])
            full_path = os.path.join(target_dir, rel_path)
            cache_full_path = os.path.join(cache_dir, rel_path)
            if len(diff['update'][encoded_path]) and diff['update'
                    ][encoded_path][0] == DIR_FLAG:
                if not os.path.exists(full_path):
                    os.makedirs(full_path)
                if os.path.exists(full_path) \
                    and os.path.isfile(full_path):
                    try:
                        os.remove(full_path)
                    except Exception as e:
                        errors.append((rel_path, e))

                return
            if os.path.exists(full_path) and os.path.isdir(full_path):
                try:
                    shutil.rmtree(full_path)
                except Exception as e:
                    errors.append((rel_path, e))
                    return

            tmp_path = os.path.split(full_path)[0]
            if not os.path.exists(tmp_path):
                os.makedirs(tmp_path)
            if os.path.exists(cache_full_path):
                digest_path(cache_full_path, download_dir, 0,
                            check_matching=set(diff['update'
                            ][encoded_path]))
            urls = []
            for hash in diff['update'][encoded_path]:
                url = src_url_root + '/' + hash[:2] + '/' + hash
                urls.append(url)

            download_queue.download(
                encoded_path,
                urls,
                download_dir,
                full_path,
                on_complete=_on_download_complete,
                on_error=_on_download_error,
                on_stats=_on_stats,
                on_stats_kwargs=stats_callback_kwargs,
                )

        if aborted[0]:
            download_queue.stop()
            return (completed, errors)
        download_file(encoded_path)

    if aborted[0]:
        download_queue.stop()
        return (completed, errors)
    paths_to_delete = []
    for encoded_path in diff['remove']:
        rel_path = decode_path(encoded_path,
                               dst_manifest['filepath_encoding'])
        full_path = os.path.join(target_dir, rel_path)
        if os.path.isfile(full_path):
            try:
                logging.debug('Removing file: ' + full_path)
                os.remove(full_path)
            except Exception as e:
                errors.append((full_path, e))
        elif os.path.isdir(full_path):

            paths_to_delete.append(full_path)

    for full_path in paths_to_delete:
        if os.path.exists(full_path):
            try:
                logging.debug('Removing path: ' + full_path)
                shutil.rmtree(full_path)
            except Exception as e:
                errors.append((full_path, e))

    while 1:
        if aborted[0]:
            break
        try:
            download_queue.join(1)
            break
        except downloader.NotFinished:
            continue

    download_queue.stop()
    return (completed, errors)


def patch(
    target_dir,
    src_manifest_path,
    dst_manifest_url,
    download_dir,
    progress_callback=None,
    diff=None,
    progress_callback_kwargs={},
    cache_dir=None,
    dirty_flag=False,
    ):
    (dst_hash, dst_manifest) = load_manifest_from_url(dst_manifest_url,
            os.path.dirname(src_manifest_path))
    aborted = [False]

    def _download_stats_callback(**kwargs):
        state = 'downloading'
        if progress_callback is not None:
            result = progress_callback(state, **kwargs)
            if result is False:
                aborted[0] = True
            return result

    def _checking_stats_callback(**kwargs):
        state = 'checking'
        if progress_callback is not None:
            result = progress_callback(state, **kwargs)
            if result is False:
                aborted[0] = True
            return result

    def _diffing_stats_callback(**kwargs):
        state = 'diffing'
        if progress_callback is not None:
            result = progress_callback(state, **kwargs)
            if result is False:
                aborted[0] = True
            return result

    def _cleanup_stats_callback(**kwargs):
        state = 'cleanup'
        if progress_callback is not None:
            result = progress_callback(state, **kwargs)
            if result is False:
                aborted[0] = True
            return result

    src_hash = None
    src_manifest = None
    try:
        (src_hash, src_manifest) = \
            load_manifest_from_file(src_manifest_path)
    except Exception as e:
        logging.exception(str(e))

    if src_manifest is None:
        src_manifest = copy_manifest(dst_manifest)
        dirty_flag = True
    if dirty_flag:
        update_manifest_from_path(src_manifest, target_dir,
                                  ignore_untracked=True,
                                  progress_callback=_checking_stats_callback,
                                  progress_callback_kwargs=progress_callback_kwargs)
    if aborted[0]:
        return
    if diff is None:
        diff = create_manifest_diff(src_manifest, dst_manifest,
                                    progress_callback=_diffing_stats_callback,
                                    progress_callback_kwargs=progress_callback_kwargs)
    url_root = '/'.join(dst_manifest_url.split('/')[:-1]) + '/' \
        + dst_manifest['product']
    if aborted[0]:
        return
    (completed, errors) = apply_patch_diff(
        dst_manifest,
        diff,
        url_root,
        target_dir,
        download_dir,
        stats_callback=_download_stats_callback,
        stats_callback_kwargs=progress_callback_kwargs,
        cache_dir=cache_dir,
        )
    if len(errors):
        raise Exception('Patch failed: \n%s' % '\n'.join([str(e)
                        for e in errors]))
    if aborted[0]:
        return completed

    def cleanup(download_dir):
        _cleanup_stats_callback()
        shutil.rmtree(download_dir, ignore_errors=True)

    save_manifest_to_file(dst_manifest, src_manifest_path)
    t = threading.Thread(target=cleanup, args=(download_dir, ))
    t.daemon = True
    t.start()
    return completed


def build(
    src_manifest,
    target_dir,
    output_dir,
    deep_check=False,
    ):
    update_manifest_from_path(src_manifest, target_dir, output_dir,
                              deep_check=deep_check, add_all=True)


def verify(
    target_dir,
    src_manifest_file,
    dst_manifest_url,
    deep_check=False,
    progress_callback=None,
    progress_callback_kwargs=None,
    ):

    def _fetching_stats_callback(**kwargs):
        state = 'fetching'
        if progress_callback is not None:
            return progress_callback(state, **kwargs)
        return True

    def _diffing_stats_callback(**kwargs):
        state = 'diffing'
        if progress_callback is not None:
            return progress_callback(state, **kwargs)
        return True

    _fetching_stats_callback(percent_complete=0)
    dst_hash = urlopen(dst_manifest_url).read().strip()
    src_hash = None
    src_manifest = None
    download_dir = None
    if src_manifest_file is not None:
        download_dir = os.path.dirname(src_manifest_file)
    try:
        (src_hash, src_manifest) = \
            load_manifest_from_file(src_manifest_file)
    except Exception as e:
        logging.warning(str(e))
        deep_check = True

    _fetching_stats_callback(percent_complete=100)
    if not deep_check:
        if src_hash != dst_hash:
            logging.debug('hash verification check failed: %s != %s'
                          % (src_hash, dst_hash))
            return (False, None)
        return (True, None)
    elif src_manifest is None:
        return (False, None)
    (dst_hash, dst_manifest) = load_manifest_from_url(dst_manifest_url,
            download_dir)
    diff = create_manifest_diff(dst_manifest, src_manifest,
                                progress_callback=_diffing_stats_callback,
                                progress_callback_kwargs=progress_callback_kwargs)
    if len(diff['update']) or len(diff['remove']):
        logging.debug('hash verification check failed: files modified')
        logging.debug('Diff: ' + json.dumps(diff, indent=4))
        return (False, diff)
    else:
        return (True, None)


def _test():
    logging_.basicConfig(level=logging_.DEBUG)
    import doctest
    doctest.testmod()


if __name__ == '__main__':
    _test()

			