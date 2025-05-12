import os
import time
import sys
import logging
try:
    sys.stdout = open("passport_updater_stdout.log", "w")
    sys.stderr = open("passport_updater_stderr.log", "w")
except:
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
    
import shutil
import subprocess
import threading
#import updaterwindow
import ctypes
import json
from fnmatch import fnmatch
from patchkit import patcher, manifest
import tracker

CREATE_NO_WINDOW = 0x08000000
g_ui_state = 0
g_progress = 0
progress = None

class NProgress(ctypes.Structure):
    _fields_ = [
        ("minimum", ctypes.c_float),
        ("speed", ctypes.c_float),
        ("trickle_rate", ctypes.c_float),
        ("trickle_speed", ctypes.c_float),
        ("n", ctypes.c_float),
        ("progress", ctypes.c_float),
        ("last_update_time", ctypes.c_double),
        ("_start_fp", ctypes.c_void_p),
        ("_set_fp", ctypes.c_void_p),
        ("_inc_fp", ctypes.c_void_p),
        ("_work_fp", ctypes.c_void_p),
        ("_done_fp", ctypes.c_void_p),
    ]
    inc = None
    progress = None

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

def swap_exe():
    exe_path = get_exe()
    exe_basename = os.path.basename(exe_path)
    filename, ext = os.path.splitext(exe_basename)
    cwd = os.path.dirname(exe_path)
    orig_filename = os.path.join(cwd,u"__tmp_exe_basename")
    if fnmatch(filename, u"*_backup") or ext.lower() == ".py":
        try:
            exe_basename = open(orig_filename,"r").read()
            os.remove(orig_filename)
            time.sleep(0.25)
            p = subprocess.Popen(
                ["taskkill", "/f", "/im", exe_basename], 
                close_fds=True, 
                creationflags=CREATE_NO_WINDOW)
                
            p.wait()
        except Exception as e:
            print e
        return
    tmp_exe_filename_file = open(orig_filename,"w")
    tmp_exe_filename_file.write(exe_basename)
    tmp_exe_filename_file.close()
    
    swap_exe_filename = filename + u"_backup" + ext
    #swap_exe_path = os.path.join(cwd,swap_exe_filename)
    #print "SWAP_EXE:", swap_exe_path
    shutil.copy2(exe_path, swap_exe_filename)
    p = subprocess.Popen(
        [swap_exe_filename] + sys.argv[1:], 
        close_fds=True, 
        creationflags=CREATE_NO_WINDOW)
        
    ctypes.windll.User32.AllowSetForegroundWindow(p.pid)
    sys.exit(0)


def do_update():

        
    #t = threading.Thread(target=_winthread)
    #t.daemon = True
    #t.start()
    #time.sleep(3)
    
    working_dir = os.path.dirname(get_exe())
    manifest_url = "http://nxeu.s3.eu-central-1.amazonaws.com/passport/patchdata/passport.manifest.hash"
    env_conf_file = os.path.join(working_dir, "env.conf")
    if os.path.exists(env_conf_file):
        env_conf = json.loads(open(env_conf_file,"r").read())
        manifest_url = env_conf.get("manifest_url", manifest_url)
    if not manifest_url:
        return
    target_dir = working_dir
    home_dir = working_dir
    temp_dir = os.path.join(home_dir, "patch", "passport")
    manifest_file = os.path.join(home_dir, "patch", "passport.manifest.hash")
    
    complete_status = {
        "percent_complete": 0,
        "state": "pending",
        "state_complete": 0,
        "done": False
    }
    
    def update_progress():
        while not complete_status["done"]:
            logging.debug(str(complete_status))
            progress.set(progress, complete_status["percent_complete"])
            time.sleep(1)
            
    t = threading.Thread(target=update_progress)
    t.daemon = True
    t.start()
    
    def progress_callback(status, *args, **kwargs):
        #print status, args, kwargs
        stage = 0
        
        if status == "checking":
            stage = 1
        elif status == "diffing":
            stage = 2
        elif status == "downloading":
            stage = 3
            
        base_percent_complete = 100*stage/4.0
        percent_complete = (kwargs.get("percent_complete", 0) or 0)
        complete_status["percent_complete"] = (base_percent_complete + 0.25*percent_complete)/100
        complete_status["state"] = status
        complete_status["state_complete"] = percent_complete
        
        
    def verify_files():
        verified = False
        try:
            verified, diff = patcher.verify(target_dir, 
                manifest_file, manifest_url, deep_check=False, 
                progress_callback=progress_callback)
            if progress_callback:
                progress_callback('verify_complete', verified=verified, diff=diff)
        except Exception as e:
            if progress_callback:
                progress_callback('exception', exception=e)
                tracker.track_event("errors", "checking-files-failed", {"label":str(e)})
            raise
        
        return verified, diff
        
    
    def patch_files(diff):
        result = None
        tracker.track_event("updater", "download-update")
        try:
            result = patcher.patch(target_dir, manifest_file, 
                manifest_url, download_dir=temp_dir, 
                dirty_flag=False, diff=diff, 
                progress_callback=progress_callback)
        except Exception as e:
            if progress_callback:
                progress_callback('exception', exception=e)
                tracker.track_event("errors", "download-update-failed", {"label": str(e)})
            raise
        if progress_callback:
            progress_callback('success', result=result)
        
    verified, diff = verify_files()
    
    
    if progress:
        progress.inc(progress, 0.2) 
    
    if verified:
        return
    
    swap_exe()
    
    
   
    patch_files(diff)
    complete_status["done"] = True
    
    
def launch_client():
    exe_path = get_exe()
    cwd = os.path.dirname(exe_path)
    client_filename = "passport_client.exe"
    p = subprocess.Popen([client_filename] + sys.argv[1:], close_fds=True)
    ctypes.windll.User32.AllowSetForegroundWindow(p.pid)
    
        
def main():
    logging.basicConfig(level=logging.DEBUG)
    global progress
    
    if g_progress:
        progress = ctypes.cast(g_progress, ctypes.POINTER(NProgress))
        inc_func_t =  ctypes.CFUNCTYPE(None, ctypes.POINTER(NProgress), ctypes.c_float)
        set_func_t =  ctypes.CFUNCTYPE(None, ctypes.POINTER(NProgress), ctypes.c_float)
        progress.inc = ctypes.cast(progress.contents._inc_fp,inc_func_t)
        progress.set = ctypes.cast(progress.contents._set_fp,set_func_t)
    
    do_update()
    launch_client()
    