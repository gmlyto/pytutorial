from pynput import mouse
from datetime import datetime, timezone
import os

# Direktori dan file log
log_directory = "F:\\PTlogcreated"  # Direktori untuk menyimpan log
log_file = os.path.join(log_directory, "mouse_clicks_with_timestamps.log")

# Pastikan direktori log ada, jika tidak, buat direktori
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Fungsi untuk mencatat log ke file
def log_click(log_message):
    with open(log_file, "a") as file:
        file.write(log_message + "\n")
    print(log_message)  # Debugging (opsional)

# Fungsi untuk menangani klik mouse
def on_click(x, y, button, pressed):
    if button == mouse.Button.left and pressed:
        # Dapatkan waktu saat ini
        now = datetime.now()
        timestamp_unix = int(now.timestamp())  # Unix Timestamp (detik)
        timestamp_unix_millis = int(now.timestamp() * 1000)  # Unix Timestamp (milidetik)
        iso_8601 = now.isoformat()  # ISO 8601 Format
        utc_time = now.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # UTC time
        local_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Local time

        # Buat log dalam format seperti di website
        log_message = (
            f"Klik kiri pada {local_time} @ {x},{y}\n"
            f"Unix Timestamp: {timestamp_unix}\n"
            f"Unix Timestamp (ms): {timestamp_unix_millis}\n"
            f"ISO 8601: {iso_8601}\n"
            f"UTC: {utc_time}\n"
            f"Local: {local_time}\n"
        )

        # Simpan log ke file
        log_click(log_message)

# Listener untuk mouse
with mouse.Listener(on_click=on_click) as listener:
    listener.join()