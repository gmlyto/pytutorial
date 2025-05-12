"""
patchkit

Usage:
    patchkit build [--force] <name> <src_dir> <output_dir>
    patchkit patch [--force] [--tempdir=<temp_dir>] <name> <manifest_url> <target_dir> 
    patchkit -h | --help

Options:

    -h --help       Show this help message.
    
"""
from docopt import docopt
import os
import sys
import patcher
import manifest
import threading
import logging



def build(product_id, src_dir, output_dir, force=False):
    m = manifest.create_manifest()
    m['product'] = product_id
    manifest_file = os.path.join(output_dir, product_id + ".manifest.hash")
    output_dir = os.path.join(output_dir, product_id)
    patcher.build(m, src_dir, output_dir, deep_check=force)
    manifest.save_manifest_to_file(m, manifest_file)

def patch(product_id, manifest_url, target_dir, 
  temp_dir=None, force=False, progress_callback=None):
    temp_dir = temp_dir or os.path.join(os.getcwd(), "patch")
    manifest_file = os.path.join(
        temp_dir, product_id + ".manifest.hash")
        
    temp_dir = os.path.join(temp_dir, product_id)
    try:
        verified, diff = patcher.verify(target_dir, 
            manifest_file, manifest_url, deep_check=force, 
            progress_callback=progress_callback)
        if progress_callback:
            progress_callback('verify_complete', verified=verified, diff=diff)
    except Exception as e:
        if progress_callback:
            progress_callback('exception', exception=e)
        raise
        
    if verified:
        return None
        
    def patch_thread():
        result = None
        try:
            result = patcher.patch(target_dir, manifest_file, 
                manifest_url, download_dir=temp_dir, 
                dirty_flag=force, diff=diff, 
                progress_callback=progress_callback)
        except Exception as e:
            if progress_callback:
                progress_callback('exception', exception=e)
            raise
        if progress_callback:
            progress_callback('success', result=result)
        #manifest.save_manifest_to_file(m, manifest_file)
    t = threading.Thread(target=patch_thread)
    t.daemon = True
    t.start()
    return t
    
logging.basicConfig(level=logging.DEBUG)
args = docopt(__doc__)

if args['build']:
    build(args['<name>'], args['<src_dir>'], 
        args['<output_dir>'], args['--force'])
if args['patch']:
    t = patch(args['<name>'], args['<manifest_url>'], 
        args['<target_dir>'], args['--tempdir'], args['--force'])
    
    if t:
        t.join()
        
        
