import sys


def open_bytes_read(path):
    return open(path, "rb") if path != "-" else sys.stdin.buffer


def open_bytes_write(path):
    return open(path, "wb") if path != "-" else sys.stdout.buffer


def open_str_read(path):
    return open(path, "r") if path != "-" else sys.stdin


def open_str_write(path):
    return open(path, "w") if path != "-" else sys.stdout
