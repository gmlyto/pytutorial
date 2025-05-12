# -*- coding: utf-8 -*-

import sys
import os
import zlib
import math
import hashlib
import json
try:
    import urllib2
    from urllib2 import urlopen
except ImportError:
    import urllib as urllib2
    from urllib.request import urlopen
import urllib3
import urllib
try:
    import urlparse
except ImportError:
    from urllib import parse as urlparse
import base64
import time
import copy
import shutil
import threading
import traceback
import logging as logging_
logging = logging_.getLogger(__name__)
import tempfile
from collections import OrderedDict
from pprint import pprint
from fnmatch import fnmatch
CONFIG = {
    'chunk_size': 4194304,
    'compression_enabled': True,
    'max_download_retries': 5,
    'num_download_threads': 10,
    'filepath_encoding': 'utf16',
    }
DIR_FLAG = '__DIR__'
VERSION = '0.5'
STATUS_CODES = {'listing_files': 1, 'checking_files': 2}


def decode_path(encoded_path, encoding):
    r"""
    >>> encoded_path = '//5DADoAXAB0AGUAcwB0AFwAcABhAHQAaABcAHQAbwBcAGYAaQBsAGUA'
    >>> print(decode_path(encoded_path, 'utf16'))
    C:\test\path\to\file
    """

    rel_path = base64.b64decode(encoded_path)
    try:
        rel_path = rel_path.decode(encoding, 'ignore')
    except Exception as e:
        logging.exception(str(e))
        logging.error('Unable to decode path %s to %s' % (rel_path,
                      encoding))

    return rel_path


def encode_path(rel_path, encoding):
    r"""
    >>> print(encode_path(r'C:\test\path\to\file', 'utf16'))
    //5DADoAXAB0AGUAcwB0AFwAcABhAHQAaABcAHQAbwBcAGYAaQBsAGUA
    """

    try:
        rel_path = rel_path.encode(encoding, 'ignore')
    except Exception as e:
        logging.exception(str(e))
        logging.error('Unable to encode path %s to %s' % (rel_path,
                      encoding))

    encoded_path = base64.b64encode(rel_path).decode('utf8')
    return encoded_path

def download_file(url):
    max_retries = 30
    retries = max_retries
    while retries:
        try:
            data = urlopen(url).read()
            return data
        except Exception as e:
            if retries == 0:
                raise
            wait_timer = 3.0*(max_retries - retries)/max_retries
            logging.error(
                "Download for %s failed. Retries left: %s (wait %0.2f) Error: %s"%(
                    url, retries, wait_timer, str(e)))
            time.sleep(wait_timer)
            retries -= 1
            
                
def hash_chunk(chunk):
    return hashlib.sha1(chunk).hexdigest()


def digest_path(
    full_path,
    output_path=None,
    compress_level=6,
    check_matching=None,
    ):
    """
    >>> import tempfile
    >>> import shutil
    >>> import time
    >>> working_dir = tempfile.mkdtemp()
    >>> digest_path(working_dir)
    (['__DIR__'], [0])
    >>> test_file = tempfile.mkstemp()
    >>> tempdata = b'ASDASDasdfasf'*(1<<19)
    >>> f = os.fdopen(test_file[0],'wb')
    >>> _ = f.write(tempdata)
    >>> f.close()
    >>> digest_path(test_file[1],working_dir)
    (['a2f5500c73eee6756541881812d055c772896859', 'c7c5d14239f94fdd799fcf6791ac6f4154f4588e'], [8166, 5120])
    >>> os.listdir(working_dir)
    ['a2', 'c7']
    >>> shutil.rmtree(working_dir)
    >>> os.remove(test_file[1])
    """

    if os.path.isdir(full_path):
        return ([DIR_FLAG], [0])
    if check_matching:
        check_matching = set(copy.copy(check_matching))
    if os.path.isfile(full_path):
        fin = open(full_path, 'rb')
        chunk = fin.read(CONFIG['chunk_size'])
        sequence = 0
        hash_infos = []
        sizes = []
        while chunk:
            hash_info = hash_chunk(chunk)
            chunk2 = None
            if check_matching and hash_info not in check_matching:
                chunk2 = fin.read(CONFIG['chunk_size'])
                search_chunk = chunk + chunk2
                found = False
                sub_offset = 0
                sub_hash = ''
                old_length = len(chunk)
                i = 1
                while i < min(len(search_chunk), CONFIG['chunk_size']):
                    sub_hash = hash_chunk(search_chunk[i:i
                            + len(chunk)])
                    if sub_hash in check_matching:
                        chunk = search_chunk[0:i]
                        sub_offset = len(chunk)
                        found = True
                        break
                    i *= 2

                if found:
                    logging.debug('Found inserted chunk ' + sub_hash
                                  + ' of size ' + str(sub_offset))
                    fin.seek(-len(chunk2), 1)
                    fin.seek(sub_offset - old_length, 1)
                    check_matching.add(hash_info)
                    continue
            if output_path:
                _output_path = os.path.join(output_path, hash_info[:2])
                output_file = os.path.join(_output_path, hash_info)
                if not os.path.exists(_output_path):
                    os.makedirs(_output_path)
                need_rebuild = True
                if os.path.exists(output_file):
                    logging.debug('Object file already exists: '
                                  + output_file)
                    try:
                        tmp_chunk = open(output_file, 'rb').read()
                        if CONFIG['compression_enabled']:
                            tmp_chunk = zlib.decompress(tmp_chunk)
                        tmp_hash = hash_chunk(tmp_chunk)
                        if tmp_hash == hash_info:
                            need_rebuild = False
                        else:
                            logging.warning('Existing object file corrupted: '
                                     + output_file)
                    except Exception as e:
                        logging.warning(str(e))

                if need_rebuild:
                    if CONFIG['compression_enabled']:
                        chunk = zlib.compress(chunk, compress_level)
                    fout = open(output_file, 'wb')
                    logging.debug('Writing object: ' + output_file)
                    fout.write(chunk)
                    fout.close()
            sizes.append(len(chunk))
            hash_infos.append(hash_info)
            chunk = chunk2 or fin.read(CONFIG['chunk_size'])
            sequence += 1

        fin.close()
        return (hash_infos, sizes)


def serialize_manifest(manifest):
    """
    >>> manifest = create_manifest()
    >>> hash, manifest_data = serialize_manifest(manifest)
    >>> _hash, _manifest = deserialize_manifest(manifest_data)
    >>> assert(_hash == hash)
    >>> assert(_manifest == manifest)
    >>> __hash, _manifest_data = serialize_manifest(_manifest)
    >>> assert(__hash == _hash)
    >>> assert(_manifest_data == manifest_data)
    """

    manifest_data = json.dumps(manifest, sort_keys=True,
                               indent=4).encode('utf8')
    hash = hash_chunk(manifest_data)
    manifest_data = zlib.compress(manifest_data)
    return (hash, manifest_data)


def deserialize_manifest(manifest_data):
    manifest_data = zlib.decompress(manifest_data)
    hash = hash_chunk(manifest_data)
    manifest = json.loads(manifest_data.decode('utf8'))
    return (hash, manifest)


def copy_manifest(src_manifest):
    return copy.deepcopy(src_manifest)


def load_manifest_from_file_hash(manifest_path):
    manifest_data = open(manifest_path, 'rb').read()
    _hash = os.path.basename(manifest_path)
    (hash, manifest) = deserialize_manifest(manifest_data)
    assert hash == _hash
    return (hash, manifest)


def load_manifest_from_file(path):
    _hash = open(path, 'rb').read().strip()
    manifest_path = os.path.join(os.path.split(path)[0], _hash)
    (hash, manifest) = load_manifest_from_file_hash(manifest_path)
    assert hash == _hash
    return (hash, manifest)


def cache_manifest(manifest_data, cache_filepath):
    cache_dir = os.path.dirname(cache_filepath)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    open(cache_filepath, 'wb').write(manifest_data)


def delete_manifest_cache(cache_filepath):
    if os.path.exists(cache_filepath):
        try:
            os.remove(cache_filepath)
        except:
            pass


def load_manifest_from_url_hash(manifest_url, cache_filepath=None):
    if cache_filepath is not None and os.path.exists(cache_filepath):
        try:
            return load_manifest_from_file_hash(cache_filepath)
        except:
            delete_manifest_cache(cache_filepath)

    manifest_data = download_file(manifest_url)
    if cache_filepath is not None:
        cache_manifest(manifest_data, cache_filepath)
    (hash, manifest) = deserialize_manifest(manifest_data)
    return (hash, manifest)


def load_manifest_from_url(url, cache_dir=None):
    _hash = download_file(url).strip().decode('utf8')
    manifest_url = '/'.join(url.split('/')[:-1]) + '/' + _hash
    cache_filepath = os.path.join(cache_dir, _hash)
    (hash, manifest) = load_manifest_from_url_hash(manifest_url,
            cache_filepath)
    assert hash == _hash
    return (hash, manifest)


def save_manifest_to_file(manifest, output_path):
    logging.debug('Saving manifest to file: ' + output_path)
    output_dir = os.path.split(output_path)[0]
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    (hash, manifest_data) = serialize_manifest(manifest)
    open(output_path, 'wb').write(hash.encode('utf8'))
    manifest_output_path = os.path.join(output_dir, hash)
    open(manifest_output_path, 'wb').write(manifest_data)


def get_file_stats(full_path):
    fstat = os.stat(full_path)
    return {'mtime': int(fstat.st_mtime), 'fsize': fstat.st_size}


def recursive_check_directory(manifest, source_dir, file_callback=None):

    def _file_callback(rel_path, status):
        if file_callback:
            return file_callback(rel_path, status)
        return True

    found_paths = set()
    for (dirpath, dirnames, filenames) in os.walk(source_dir):
        for filename in filenames + dirnames:
            full_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(full_path, source_dir)
            logging.debug('Checking: ' + rel_path)
            try:
                encoded_path = encode_path(rel_path,
                        manifest['filepath_encoding'])
            except Exception as e:
                logging.exception(str(e))
                logging.error('Unable to encode file name: ' + rel_path)
                if _file_callback(rel_path, 'encode_error') is False:
                    return
                continue

            found_paths.add(encoded_path)
            exists_in_manifest = encoded_path in manifest['files']
            if exists_in_manifest:
                fstat = os.stat(full_path)
                if int(fstat.st_size) == manifest['files'
                        ][encoded_path]['fsize'] \
                    and int(fstat.st_mtime) == manifest['files'
                        ][encoded_path]['mtime']:
                    logging.debug('File clean: ' + rel_path)
                    if _file_callback(rel_path, 'fstat_not_modified') \
                        is False:
                        return
                else:
                    logging.debug('File modified: ' + rel_path)
                    if _file_callback(rel_path, 'fstat_modified') \
                        is False:
                        return
            else:
                logging.debug('File not tracked: ' + rel_path)
                if _file_callback(rel_path, 'not_tracked') is False:
                    return
    encoded_paths = manifest['files'].copy().keys()
    for encoded_path in encoded_paths:
        rel_path = decode_path(encoded_path,
                               manifest['filepath_encoding'])
        if encoded_path not in found_paths:
            logging.debug('File does not exist: ' + rel_path)
            if _file_callback(rel_path, 'does_not_exist') is False:
                return


def update_manifest_from_path(
    manifest,
    source_dir,
    output_dir=None,
    deep_check=False,
    add_all=False,
    ignore_untracked=False,
    progress_callback=None,
    progress_callback_kwargs={},
    ):
    progress = {'total_items': len(manifest['files']),
                'current_item': 0, 'percent_complete': 0}
    total_objects = [0]
    total_uncompressed_size = [0]
    progress.update(progress_callback_kwargs)

    def _update_file_callback(rel_path, status):
        encoded_path = encode_path(rel_path,
                                   manifest['filepath_encoding'])
        full_path = os.path.join(source_dir, rel_path)
        file_needs_update = status in ('fstat_modified', 'not_tracked') \
            or deep_check and status in ('fstat_not_modified', )
        if ignore_untracked and status == 'not_tracked':
            file_needs_update = False
        if file_needs_update:
            fstat = get_file_stats(full_path)
            total_uncompressed_size[0] += fstat['fsize']
            (objects, sizes) = digest_path(full_path, output_dir)
            manifest['files'][encoded_path] = {
                'mtime': fstat['mtime'],
                'fsize': fstat['fsize'],
                'objects': objects,
                'objects_fsize': sizes,
                }
            total_objects[0] += len(manifest['files'
                                    ][encoded_path]['objects'])
        if encoded_path in manifest['files']:
            progress['current_item'] += 1
        if status in ('does_not_exist', ):
            del manifest['files'][encoded_path]
            progress['total_items'] = len(manifest['files'])
        if progress['total_items']:
            progress['percent_complete'] = min(100
                    * progress['current_item'] / progress['total_items'
                    ], 100)
        if progress_callback is not None:
            if progress_callback(**progress) is False:
                logging.info('Update manifest aborted')
                return False
        return True

    recursive_check_directory(manifest, source_dir,
                              _update_file_callback)
    manifest['buildtime'] = time.time()
    manifest['total_uncompressed_size'] = total_uncompressed_size[0]
    manifest['total_objects'] = total_objects[0]


def create_manifest():
    manifest = {}
    manifest['product'] = '0'
    manifest['platform'] = None
    manifest['buildtime'] = time.time()
    manifest['version'] = VERSION
    manifest['files'] = {}
    manifest['total_uncompressed_size'] = 0
    manifest['total_compressed_size'] = 0
    manifest['total_objects'] = 0
    manifest['filepath_encoding'] = CONFIG['filepath_encoding']
    return manifest


def _estimate_object_diff_size(src_objects, dst_objects, dst_file_size):
    total_size = 0
    changes = set(dst_objects) - set(src_objects)
    if len(dst_objects) and dst_objects[-1] in changes:
        total_size += dst_file_size % CONFIG['chunk_size']
        changes.remove(dst_objects[-1])
    total_size += len(changes) * CONFIG['chunk_size']
    return total_size


def create_manifest_diff(
    src_manifest,
    dst_manifest,
    progress_callback=None,
    progress_callback_kwargs={},
    ):
    total_size = 0
    patch_size = 0
    update = {}
    remove = []
    current_item = 0
    progress = {'percent_complete': 0, 'current_item': 0,
                'total_items': len(dst_manifest['files']) \
                + len(src_manifest['files'])}
    if progress_callback_kwargs is not None:
        progress.update(progress_callback_kwargs)
    for encoded_path in dst_manifest['files']:
        rel_path = decode_path(encoded_path,
                               dst_manifest['filepath_encoding'])
        src_objects = src_manifest['files'].get(encoded_path,
                {'objects': []})['objects']
        dst_objects = dst_manifest['files'][encoded_path]['objects']
        dst_filesize = dst_manifest['files'][encoded_path]['fsize']
        if encoded_path not in src_manifest['files'] \
            or len(dst_objects) != len(src_objects):
            update[encoded_path] = dst_objects
            patch_size += _estimate_object_diff_size(src_objects,
                    dst_objects, dst_filesize)
            total_size += dst_filesize
        else:
            for i in range(0, len(dst_objects)):
                if dst_objects[i] != src_objects[i]:
                    update[encoded_path] = dst_manifest['files'
                            ][encoded_path]['objects']
                    patch_size += \
                        _estimate_object_diff_size(src_objects,
                            dst_objects, dst_filesize)
                    total_size += dst_filesize
                    break

        progress['current_item'] += 1
        if progress['total_items']:
            progress['percent_complete'] = 100.0 \
                * progress['current_item'] / progress['total_items']
        if progress_callback is not None:
            if progress_callback(**progress) is False:
                logging.info('Diffing aborted')
                return

    for encoded_path in src_manifest['files']:
        rel_path = decode_path(encoded_path,
                               src_manifest['filepath_encoding'])
        if encoded_path not in dst_manifest['files']:
            remove.append(encoded_path)
        progress['current_item'] += 1
        if progress['total_items']:
            progress['percent_complete'] = 100.0 \
                * progress['current_item'] / progress['total_items']
        if progress_callback is not None:
            if progress_callback(**progress) is False:
                logging.info('Diffing aborted')
                return

    return {
        'update': OrderedDict(sorted(update.items(), key=lambda x: \
                              -len(x[1]))),
        'remove': remove,
        'total_size': total_size,
        'patch_size': patch_size,
        }


def _test():
    logging_.basicConfig(level=logging_.DEBUG)
    import doctest
    doctest.testmod()


if __name__ == '__main__':
    _test()

			