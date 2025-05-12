import os
import sys
import urllib
import urllib2
import cookielib
import sqlite3
import uuid
import logging
import time
import ctypes
import threading
import platform
from Queue import Queue
from ctypes import wintypes, windll

def getenv(name):
    name = unicode(name)
    n = ctypes.windll.kernel32.GetEnvironmentVariableW(name, None, 0)
    if not n:
        return None
    buf = ctypes.create_unicode_buffer(n)
    ctypes.windll.kernel32.GetEnvironmentVariableW(name, buf, n)
    return buf.value
    
def get_exe():
    return getenv(u'NXEU_EXE_FILE') or sys.executable

def get_local_appdata_dir():
    CSIDL_LOCAL_APPDATA = 0x1C

    _SHGetFolderPath = windll.shell32.SHGetFolderPathW
    _SHGetFolderPath.argtypes = [wintypes.HWND,
                                ctypes.c_int,
                                wintypes.HANDLE,
                                wintypes.DWORD, wintypes.LPCWSTR]
                                
    path_buf = wintypes.create_unicode_buffer(wintypes.MAX_PATH)
    result = _SHGetFolderPath(0, CSIDL_LOCAL_APPDATA, 0, 0, path_buf)
    return path_buf.value



ga_client_uuid = None
ga_tracking_id = 'UA-74898-39'


def get_cid_from_db():
    try:
        db_file = os.path.join(
            get_local_appdata_dir(), 
            "NXEPassportClient", 
            "Local Storage", 
            "app_client_0.localstorage")
            
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        c.execute('SELECT value FROM `ItemTable` WHERE key=?;', ('client_uuid',))
        return unicode(c.fetchone()[0], 'utf16')
    except Exception as e:
        logging.error(str(e))
        return None
        
def get_cid_from_file():
    try:
        working_dir = os.path.dirname(get_exe())
        uuid_file = os.path.join(working_dir, 'cid.dat')
        return open(uuid_file,'r').read()
    except Exception as e:
        logging.error(str(e))
        return None
        
def save_cid_to_file():
    global ga_client_uuid
    if not ga_client_uuid:
        return
    try:
        working_dir = os.path.dirname(get_exe())
        uuid_file = os.path.join(working_dir, 'cid.dat')
        f = open(uuid_file, 'w')
        f.write(ga_client_uuid)
        f.close()
    except Exception as e:
        logging.error(e)

def gen_cid():
    global ga_client_uuid
    ga_client_uuid = str(uuid.uuid4())
    

def load_cid():
    global ga_client_uuid
    if ga_client_uuid is not None:
        return
        
    ga_client_uuid = get_cid_from_db()
    if ga_client_uuid is None:
        ga_client_uuid = get_cid_from_file()
    if ga_client_uuid is None:
        gen_cid()
        save_cid_to_file()


def get_user_agent():
    windows_version = '.'.join(platform.version().split('.')[:2])
    return 'Mozilla/5.0 (Windows NT %s) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'%(windows_version)
    
event_queue = Queue()
def worker():
    cj = cookielib.CookieJar()
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    opener.addheaders = [
        ('User-Agent', get_user_agent())
    ]
    while True:
        try:
            url = event_queue.get()
            resp = opener.open(url).read()
            event_queue.task_done()
        except Exception as e:
            logging.error(e)
            time.sleep(0.01)
            
worker_thread = None

def enable_worker():
    global worker_thread
    if worker_thread:
        return
    worker_thread = threading.Thread(target=worker)
    worker_thread.daemon = True
    worker_thread.start()
    


def track(type, params):
    enable_worker()
    
    
    load_cid()
    base_params = [
        ("v", 1),
        ("tid", ga_tracking_id),
        ("cid", ga_client_uuid),
        ("t", type),
        ("ds", "passport_client"),
    ]
    
    collect_url = "https://ssl.google-analytics.com/collect?" + urllib.urlencode(base_params) + "&" + urllib.urlencode(params)
    
    collect_url += "&z="+str(int(time.time()*1000))
    event_queue.put(collect_url)
    
def track_event(event_category, event_action, event_data=None):
    print "Event:",event_category,event_action,event_data
    params = {
        'ec': event_category,
        'ea': event_action
    }
    if event_data:
        if event_data.get('label'):
            params['el'] = event_data['label']
            
        if event_data.get('value'):
            params['ev'] = event_data['value']
    
    track('event', params);
    
    page = '/' + event_category + '/' + event_action
    
    if event_data:
        page += '?'+urllib.urlencode(event_data)
    
    
def track_pageview(page):
    params = {
        'dh': 'nexoneu.com',
        'dp': page
    }
    track('pageview', params)