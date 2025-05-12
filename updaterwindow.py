import win32con
import time
import threading
import sys
from ctypes import *
import commctrl

WNDPROC = WINFUNCTYPE(c_long, c_int, c_uint, c_int, c_int)
NULL = c_int(win32con.NULL)


class WNDCLASS(Structure):
    _fields_ = [('style', c_uint),
                ('lpfnWndProc', WNDPROC),
                ('cbClsExtra', c_int),
                ('cbWndExtra', c_int),
                ('hInstance', c_int),
                ('hIcon', c_int),
                ('hCursor', c_int),
                ('hbrBackground', c_int),
                ('lpszMenuName', c_char_p),
                ('lpszClassName', c_char_p)]

class RECT(Structure):
    _fields_ = [('left', c_long),
                ('top', c_long),
                ('right', c_long),
                ('bottom', c_long)]

class PAINTSTRUCT(Structure):
    _fields_ = [('hdc', c_int),
                ('fErase', c_int),
                ('rcPaint', RECT),
                ('fRestore', c_int),
                ('fIncUpdate', c_int),
                ('rgbReserved', c_char * 32)]

class POINT(Structure):
    _fields_ = [('x', c_long),
                ('y', c_long)]
    
class MSG(Structure):
    _fields_ = [('hwnd', c_int),
                ('message', c_uint),
                ('wParam', c_int),
                ('lParam', c_int),
                ('time', c_int),
                ('pt', POINT)]

class BLENDFUNCTION(Structure):
    _fields_ = [('BlendOp', c_ubyte),
                ('BlendFlags', c_ubyte),
                ('SourceConstantAlpha', c_ubyte),
                ('AlphaFormat', c_ubyte)]

class BITMAP(Structure):
    _fields_ = [
       ('bmType'      , c_long),    
       ('bmWidth'     , c_long),    
       ('bmHeight'    , c_long),    
       ('bmWidthBytes', c_long),    
       ('bmPlanes'    , c_ushort),  
       ('bmBitsPixel' , c_ushort),  
       ('bmBits'      , c_void_p),
    ]

def ErrorIfZero(handle):
    if handle == 0:
        raise WinError
    else:
        return handle

def alpha_blend_window(hwnd, hbmp):
    bm = BITMAP()
    windll.gdi32.GetObjectA(hbmp, sizeof(bm), byref(bm))
    sizeSplash = POINT(bm.bmWidth, bm.bmHeight)

    ptZero = POINT(0,0)
    winrect = RECT()
    windll.user32.GetWindowRect(hwnd, byref(winrect))
    ptOrigin = POINT()
    ptOrigin.x = winrect.left
    ptOrigin.y = winrect.top
    print "Window Origin",  ptOrigin.x, ptOrigin.y
    hdcScreen = windll.user32.GetDC(NULL)
    hdcMem = windll.gdi32.CreateCompatibleDC(hdcScreen)
    hbmpOld = windll.gdi32.SelectObject(hdcMem, hbmp)
    print "HDC", hdcScreen, hdcMem
    blend = BLENDFUNCTION()
    blend.BlendOp = win32con.AC_SRC_OVER
    blend.SourceConstantAlpha = 255
    blend.AlphaFormat = win32con.AC_SRC_ALPHA

    result = windll.user32.UpdateLayeredWindow(hwnd, hdcScreen, byref(ptOrigin), byref(sizeSplash),
        hdcMem, byref(ptZero), 0, byref(blend), win32con.ULW_ALPHA)
    
    print "UpdateLayeredWindow result", result
    print "GetLastError", windll.kernel32.GetLastError()
    windll.gdi32.SelectObject(hdcMem, hbmpOld)
    windll.gdi32.DeleteDC(hdcMem)
    windll.user32.ReleaseDC(NULL, hdcScreen)
    
    
class UpdaterWindow():
    
    def _wndproc(self, hwnd, message, wParam, lParam):
        if message == win32con.WM_CREATE:
            return 0
        if message == win32con.WM_PAINT:
            return 0
        if message == win32con.WM_SIZE:
            return 0
        if message == win32con.WM_DESTROY:
            windll.user32.PostQuitMessage(0)
            return 0
            
        return windll.user32.DefWindowProcA(c_int(hwnd), c_int(message), c_int(wParam), c_int(lParam))
        
    def __init__(self):
    
        self.progress = {
            "complete": 0
        }
        self.width = 200
        self.height = 120
        self.screen_width = windll.user32.GetSystemMetrics(win32con.SM_CXSCREEN)
        self.screen_height = windll.user32.GetSystemMetrics(win32con.SM_CYSCREEN)
        self.tray_hwnd = windll.user32.FindWindowA('Shell_TrayWnd','')
        self.tray_rect = RECT()
        windll.user32.GetWindowRect(self.tray_hwnd, byref(self.tray_rect))
        
        CreateWindowEx = windll.user32.CreateWindowExA
        CreateWindowEx.argtypes = [c_int, c_char_p, c_char_p, c_int, c_int, c_int, c_int, c_int, c_int, c_int, c_int, c_int]
        CreateWindowEx.restype = ErrorIfZero

        self.wndclass = WNDCLASS()
        self.wndclass.style = win32con.CS_HREDRAW | win32con.CS_VREDRAW 
        self.wndclass.lpfnWndProc = WNDPROC(self._wndproc)
        self.wndclass.cbClsExtra = self.wndclass.cbWndExtra = 0
        self.wndclass.hInstance = windll.kernel32.GetModuleHandleA(c_int(win32con.NULL))
        self.wndclass.hIcon = windll.user32.LoadIconA(c_int(win32con.NULL), c_int(win32con.IDI_APPLICATION))
        self.wndclass.hCursor = windll.user32.LoadCursorA(c_int(win32con.NULL), c_int(win32con.IDC_ARROW))
        self.background_bmp = windll.user32.LoadBitmapA(self.wndclass.hInstance,"BACKGROUND")
        #self.wndclass.hbrBackground = windll.gdi32.CreatePatternBrush(self.background_bmp)
        self.wndclass.hbrBackground = win32con.COLOR_BACKGROUND
            
        self.wndclass.lpszMenuName = None
        self.wndclass.lpszClassName = "UpdaterWindow"

                                             
        if not windll.user32.RegisterClassA(byref(self.wndclass)):
            raise WinError()
            
        self.hwnd = CreateWindowEx(
            win32con.WS_EX_LAYERED | win32con.WS_EX_TOOLWINDOW,
            self.wndclass.lpszClassName,
            "Updater Window",
            win32con.WS_OVERLAPPED | win32con.WS_POPUP,
            self.screen_width - self.width, self.tray_rect.top - self.height, self.width, self.height,
            win32con.NULL,
            win32con.NULL,
            self.wndclass.hInstance,
            win32con.NULL)
            
        
        
        windll.user32.ShowWindow(c_int(self.hwnd), c_int(win32con.SW_SHOWNORMAL))
        windll.user32.UpdateWindow(c_int(self.hwnd))
        
        
        
 
        
        self.progress_bar_hwnd = CreateWindowEx(win32con.WS_EX_COMPOSITED,
            commctrl.PROGRESS_CLASS, 
            None,
            win32con.WS_CHILD | win32con.WS_VISIBLE,
            20, self.height-32, self.width-40, 16,
            self.hwnd, 0, 0, win32con.NULL);
        
        windll.user32.ShowWindow(self.progress_bar_hwnd, win32con.SW_SHOWNORMAL)
        windll.user32.UpdateWindow(self.progress_bar_hwnd)
        windll.user32.SendMessageA(self.progress_bar_hwnd, commctrl.PBM_SETSTEP, 1, 0); 
        
        alpha_blend_window(self.hwnd, self.background_bmp)
        
    def pump_messages(self):
        NULL = c_int(win32con.NULL)
        msg = MSG()
        pMsg = pointer(msg)
        if windll.user32.PeekMessageA(pMsg, NULL, 0, 0, True):
            windll.user32.TranslateMessage(pMsg)
            windll.user32.DispatchMessageA(pMsg)

    def main_loop(self):
        while 1:
            self.pump_messages()
            time.sleep(0.001)
            
    def update_progress(self, progress):
        self.progress.update(progress)
        windll.user32.SendMessageA(self.progress_bar_hwnd, commctrl.PBM_SETPOS, int(self.progress.get('complete',0) or 0), 0)

if __name__=='__main__':
    app = UpdaterWindow()
    app.main_loop()