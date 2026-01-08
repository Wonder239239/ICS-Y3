from socket import *
import json
import os
from os.path import join, getsize
import hashlib
import argparse
import threading
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import base64
import uuid
import math
import shutil
import struct
from tqdm import tqdm

def get_time_based_filename(ext, prefix='', t=None):
    """
    Get a filename based on time
    :param ext: ext name of the filename
    :param prefix: prefix of the filename
    :param t: the specified time if necessary, the default is the current time. Unix timestamp
    :return:
    """
    ext = ext.replace('.', '')
    if t is None:
        t = time.time()
    if t > 4102464500:
        t = t / 1000
    return time.strftime(f"{prefix}%Y%m%d%H%M%S." + ext, time.localtime(t))


MAX_PACKET_SIZE = 20480

# Const Value
OP_SAVE, OP_DELETE, OP_GET, OP_UPLOAD, OP_DOWNLOAD, OP_BYE, OP_LOGIN, OP_ERROR = 'SAVE', 'DELETE', 'GET', 'UPLOAD', 'DOWNLOAD', 'BYE', 'LOGIN', "ERROR"
TYPE_FILE, TYPE_DATA, TYPE_AUTH, DIR_EARTH = 'FILE', 'DATA', 'AUTH', 'EARTH'
FIELD_OPERATION, FIELD_DIRECTION, FIELD_TYPE, FIELD_USERNAME, FIELD_PASSWORD, FIELD_TOKEN = 'operation', 'direction', 'type', 'username', 'password', 'token'
FIELD_KEY, FIELD_SIZE, FIELD_TOTAL_BLOCK, FIELD_MD5, FIELD_BLOCK_SIZE = 'key', 'size', 'total_block', 'md5', 'block_size'
FIELD_STATUS, FIELD_STATUS_MSG, FIELD_BLOCK_INDEX = 'status', 'status_msg', 'block_index'
DIR_REQUEST, DIR_RESPONSE = 'REQUEST', 'RESPONSE'

def set_logger(logger_name):
    """
    Create and configure a logger
    :param logger_name: logger name
    :return: logger
    """
    logger_ = logging.getLogger(logger_name)  # use named logger instead of root logger
    logger_.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '\033[0;34m%s\033[0m' % '%(asctime)s-%(name)s[%(levelname)s] %(message)s @ %(filename)s[%(lineno)d]',
        datefmt='%Y-%m-%d %H:%M:%S')

    # --> LOG FILE
    logger_file_name = get_time_based_filename('log')
    os.makedirs(f'log/{logger_name}', exist_ok=True)

    fh = TimedRotatingFileHandler(filename=f'log/{logger_name}/log', when='D', interval=1, backupCount=1)
    fh.setFormatter(formatter)

    fh.setLevel(logging.INFO)

    # --> SCREEN DISPLAY
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    logger_.propagate = False
    logger_.addHandler(ch)
    logger_.addHandler(fh)
    return logger_

# Logger
logger = set_logger('STEP-Client')


def _argparse():
    parse = argparse.ArgumentParser()
    parse.add_argument("--server_ip", required=True, help="Server IP address")
    parse.add_argument("--id", required=True, help="Student ID")
    parse.add_argument("--f", required=False, help="Path to the file to upload")
    # additional for benchmark
    parse.add_argument("--files", nargs="+", required=False, help="Paths to multiple files to upload")
    parse.add_argument(
        "--block-workers",
        type=int,
        default=1,
        help="Number of worker threads for block-level parallel upload (default: 1)."
    )
    args = parse.parse_args()
    if not args.f and not args.files:
        parse.error("You must provide at least one file via --f or --files.")
    return args


def get_file_md5(filename):
    """
    Get MD5 value for big file
    :param filename:
    :return:
    """
    m = hashlib.md5()
    with open(filename, 'rb') as fid:
        while True:
            d = fid.read(2048)
            if not d:
                break
            m.update(d)
    return m.hexdigest()


def make_packet(json_data, bin_data=None):
    """
    Make a packet following the STEP protocol.
    Any information or data for TCP transmission has to use this function to get the packet.
    :param json_data:
    :param bin_data:
    :return:
        The complete binary packet
    """
    j = json.dumps(dict(json_data), ensure_ascii=False)
    j_len = len(j)
    if bin_data is None:
        return struct.pack('!II', j_len, 0) + j.encode()
    else:
        return struct.pack('!II', j_len, len(bin_data)) + j.encode() + bin_data


def get_tcp_packet(conn):
    """
    Receive a complete TCP "packet" from a TCP stream and get the json data and binary data.
    :param conn: the TCP connection
    :return:
        json_data
        bin_data
    """
    bin_data = b''
    while len(bin_data) < 8:
        data_rec = conn.recv(8)
        if data_rec == b'':
            time.sleep(0.01)
        if data_rec == b'':
            return None, None
        bin_data += data_rec
    data = bin_data[:8]
    # cut the head
    bin_data = bin_data[8:]
    j_len, b_len = struct.unpack('!II', data)
    while len(bin_data) < j_len:
        data_rec = conn.recv(j_len)
        if data_rec == b'':
            time.sleep(0.01)
        if data_rec == b'':
            return None, None
        bin_data += data_rec
    j_bin = bin_data[:j_len]
    try:
        json_data = json.loads(j_bin.decode())
    except Exception as ex:
        return None, None
    # cut json data
    bin_data = bin_data[j_len:]
    while len(bin_data) < b_len:
        data_rec = conn.recv(b_len)
        if data_rec == b'':
            time.sleep(0.01)
        if data_rec == b'':
            return None, None
        bin_data += data_rec
    return json_data, bin_data


def make_password(student_id):
    """
    Generate MD5 password (32-char lowercase hex) from student_id per protocol.
    """
    return hashlib.md5(student_id.encode()).hexdigest()


def send_packet(sock, json_obj, bin_data=None):
    """
    Serialize and send one STEP protocol packet.
    """
    sock.sendall(make_packet(json_obj, bin_data))


def recv_packet(sock):
    """
    Receive and parse one protocol packet. Returns (json_data, bin_data) or (None, None) on error.
    """
    return get_tcp_packet(sock)


def validate_response(resp, *, expected_operation, expected_type, expected_direction=DIR_RESPONSE,
                      expected_status=200, required_fields=None, match_fields=None):
    """
    Validate STEP protocol response fields.
    Return (True, None) if everything matches; otherwise (False, error_message).
    """
    if resp is None:
        return False, 'no response from server'

    if resp.get(FIELD_OPERATION) != expected_operation:
        return False, f'unexpected operation: expected {expected_operation}, got {resp.get(FIELD_OPERATION)}'

    if resp.get(FIELD_DIRECTION) != expected_direction:
        return False, f'unexpected direction: expected {expected_direction}, got {resp.get(FIELD_DIRECTION)}'

    if resp.get(FIELD_TYPE) != expected_type:
        return False, f'unexpected type: expected {expected_type}, got {resp.get(FIELD_TYPE)}'

    status = resp.get(FIELD_STATUS)
    if status != expected_status:
        status_msg = resp.get(FIELD_STATUS_MSG, 'Unknown error')
        return False, f'status {status} != {expected_status}: {status_msg}'

    required_fields = required_fields or []
    for field in required_fields:
        if field not in resp:
            return False, f'missing field: {field}'

    match_fields = match_fields or {}
    for field, expected in match_fields.items():
        if resp.get(field) != expected:
            return False, f'mismatched field {field}: expected {expected}, got {resp.get(field)}'

    return True, None


def login(sock, student_id):
    """
    Perform login. On success return (token, resp_json); on failure return (None, resp_json or None).
    """
    password = make_password(student_id)
    login_req = {
        FIELD_TYPE: TYPE_AUTH,
        FIELD_OPERATION: OP_LOGIN,
        FIELD_DIRECTION: DIR_REQUEST,
        FIELD_USERNAME: student_id,
        FIELD_PASSWORD: password
    }
    logger.info(f'Sending LOGIN request for user {student_id}.')
    send_packet(sock, login_req)
    resp, _ = recv_packet(sock)
    ok, err = validate_response(
        resp,
        expected_operation=OP_LOGIN,
        expected_type=TYPE_AUTH,
        required_fields=[FIELD_TOKEN]
    )
    if not ok:
        logger.error(f'LOGIN failed: {err}')
        return None, resp
    token = resp[FIELD_TOKEN]
    logger.info(f'\033[1;32mLogin successful. Token: {token}\033[0m')
    return token, resp


def request_save(sock, token, filename, size):
    """
    Request upload plan. On success return a dict with key, block_size, total_block; otherwise (None, resp).
    """
    save_req = {
        FIELD_TYPE: TYPE_FILE,
        FIELD_OPERATION: OP_SAVE,
        FIELD_DIRECTION: DIR_REQUEST,
        FIELD_TOKEN: token,
        FIELD_KEY: os.path.basename(filename),
        FIELD_SIZE: size
    }
    logger.info(f'Sending SAVE request for file {save_req[FIELD_KEY]} (size: {size}).')
    send_packet(sock, save_req)
    resp, _ = recv_packet(sock)
    ok, err = validate_response(
        resp,
        expected_operation=OP_SAVE,
        expected_type=TYPE_FILE,
        required_fields=[FIELD_KEY, FIELD_BLOCK_SIZE, FIELD_TOTAL_BLOCK]
    )
    if not ok:
        logger.error(f'SAVE response invalid: {err}')
        return None, resp
    plan = {
        FIELD_KEY: resp[FIELD_KEY],
        FIELD_BLOCK_SIZE: resp[FIELD_BLOCK_SIZE],
        FIELD_TOTAL_BLOCK: resp[FIELD_TOTAL_BLOCK]
    }
    logger.info(f'Upload plan received: key={plan[FIELD_KEY]}, block_size={plan[FIELD_BLOCK_SIZE]}, total_block={plan[FIELD_TOTAL_BLOCK]}')
    return plan, resp


def upload_blocks(sock, server_ip, server_port, token, key, block_size, total_block, file_path, file_size, metrics=None, block_workers=1):
    """
    Upload the file in blocks. Return True on success, False on failure.
    """
    if metrics is not None:
        metrics.setdefault('blocks_sent', 0)
        metrics.setdefault('bytes_sent', 0)
        metrics.setdefault('block_failures', 0)

    progress = tqdm(
        total=total_block,
        unit='block',
        unit_scale=True,
        desc=f'Uploading {os.path.basename(file_path)}',
        leave=True
    )

    if block_workers <= 1:
        with open(file_path, 'rb') as f:
            for block_index in range(total_block):
                data = f.read(block_size)
                upload_req = {
                    FIELD_TYPE: TYPE_FILE,
                    FIELD_OPERATION: OP_UPLOAD,
                    FIELD_DIRECTION: DIR_REQUEST,
                    FIELD_TOKEN: token,
                    FIELD_KEY: key,
                    FIELD_BLOCK_INDEX: block_index
                }
                logger.debug(f'Sending UPLOAD block {block_index} for key {key}.')
                send_packet(sock, upload_req, data)
                resp, _ = recv_packet(sock)
                ok, err = validate_response(
                    resp,
                    expected_operation=OP_UPLOAD,
                    expected_type=TYPE_FILE,
                    required_fields=[FIELD_KEY, FIELD_BLOCK_INDEX],
                    match_fields={
                        FIELD_KEY: key,
                        FIELD_BLOCK_INDEX: block_index
                    }
                )
                if not ok:
                    logger.error(f'UPLOAD block {block_index} failed: {err}')
                    if metrics is not None:
                        metrics['block_failures'] += 1
                    progress.close()
                    return False

                if metrics is not None:
                    metrics['blocks_sent'] += 1
                    metrics['bytes_sent'] += len(data)

                progress.update(1)
        progress.close()
        return True

    # block-level parallel upload 
    worker_count = max(1, block_workers)
    index_lock = threading.Lock()
    stop_event = threading.Event()
    progress_lock = threading.Lock()
    metrics_lock = threading.Lock()
    failure_info = {"message": None}
    state = {"next_index": 0}

    def worker():
        try:
            worker_sock = socket(AF_INET, SOCK_STREAM)
            worker_sock.connect((server_ip, server_port))
        except Exception as exc:
            logger.error(f'Worker failed to connect: {exc}')
            stop_event.set()
            failure_info["message"] = f'worker connection error: {exc}'
            return

        try:
            with open(file_path, 'rb') as f:
                while not stop_event.is_set():
                    with index_lock:
                        if state["next_index"] >= total_block:
                            break
                        block_index = state["next_index"]
                        state["next_index"] += 1

                    offset = block_index * block_size
                    remaining = file_size - offset
                    read_size = block_size if block_index != total_block - 1 else remaining
                    f.seek(offset)
                    data = f.read(read_size)

                    upload_req = {
                        FIELD_TYPE: TYPE_FILE,
                        FIELD_OPERATION: OP_UPLOAD,
                        FIELD_DIRECTION: DIR_REQUEST,
                        FIELD_TOKEN: token,
                        FIELD_KEY: key,
                        FIELD_BLOCK_INDEX: block_index
                    }

                    send_packet(worker_sock, upload_req, data)
                    resp, _ = recv_packet(worker_sock)
                    ok, err = validate_response(
                        resp,
                        expected_operation=OP_UPLOAD,
                        expected_type=TYPE_FILE,
                        required_fields=[FIELD_KEY, FIELD_BLOCK_INDEX],
                        match_fields={
                            FIELD_KEY: key,
                            FIELD_BLOCK_INDEX: block_index
                        }
                    )
                    if not ok:
                        logger.error(f'UPLOAD block {block_index} failed: {err}')
                        stop_event.set()
                        failure_info["message"] = err
                        if metrics is not None:
                            with metrics_lock:
                                metrics['block_failures'] += 1
                        break

                    if metrics is not None:
                        with metrics_lock:
                            metrics['blocks_sent'] += 1
                            metrics['bytes_sent'] += len(data)

                    with progress_lock:
                        progress.update(1)
        finally:
            worker_sock.close()

    threads = []
    for _ in range(worker_count):
        th = threading.Thread(target=worker, daemon=True)
        threads.append(th)
        th.start()

    for th in threads:
        th.join()

    progress.close()

    if stop_event.is_set():
        logger.error(f'Parallel upload aborted: {failure_info["message"]}')
        return False

    return True


def verify_upload(sock, token, key, file_path):
    """
    Send GET to verify upload. Return (server_md5, resp_json) or (None, resp_json/None) on failure.
    """
    logger.info('All file blocks sent. Sending GET request to verify.')
    get_req = {
        FIELD_TYPE: TYPE_FILE,
        FIELD_OPERATION: OP_GET,
        FIELD_DIRECTION: DIR_REQUEST,
        FIELD_TOKEN: token,
        FIELD_KEY: key
    }
    send_packet(sock, get_req)
    resp, _ = recv_packet(sock)
    ok, err = validate_response(
        resp,
        expected_operation=OP_GET,
        expected_type=TYPE_FILE,
        required_fields=[FIELD_KEY, FIELD_MD5],
        match_fields={FIELD_KEY: key}
    )
    if not ok:
        logger.error(f'GET verification failed: {err}')
        return None, resp
    server_md5 = resp[FIELD_MD5]
    local_md5 = get_file_md5(file_path)
    logger.info(f'Local MD5:  {local_md5}')
    logger.info(f'Server MD5: {server_md5}')
    return server_md5, resp


def tcp_sender(server_ip, student_id, file_path, *, block_workers=1):
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' does not exist.")
        logger.error(f'File not found: {file_path}')
        return None
    # password is md5 of student_id
    password = make_password(student_id)
    file_size = os.path.getsize(file_path)
    logger.info(f'File size: {file_size} bytes for {file_path}')
    metrics = {
        'server_ip': server_ip,
        'student_id': student_id,
        'file_path': file_path,
        'file_size_bytes': file_size
    }
    total_start = time.perf_counter()

    with socket(AF_INET, SOCK_STREAM) as sock:
        logger.info(f'Connecting to server {server_ip}:1379')
        sock.connect((server_ip, 1379)) 
        logger.info('Connected to server.')
        token, login_resp = login(sock, student_id)
        if token is None:
            print(f"Login failed: {None if login_resp is None else login_resp.get('status_msg', 'Unknown error')}")
            return None

        plan, save_resp = request_save(sock, token, file_path, file_size)
        if plan is None:
            print(f"SAVE failed: {None if save_resp is None else save_resp.get('status_msg', 'Unknown error')}")
            logger.error(f'SAVE failed: {None if save_resp is None else save_resp.get("status_msg", "Unknown error")}')
            return None
        key = plan[FIELD_KEY]
        block_size = plan[FIELD_BLOCK_SIZE]
        total_block = plan[FIELD_TOTAL_BLOCK]
        print(f"Upload plan: key={key}, block_size={block_size}, total_block={total_block}")

        metrics['block_size_bytes'] = block_size
        metrics['total_blocks'] = total_block
        upload_start = time.perf_counter()
        ok = upload_blocks(
            sock,
            server_ip,
            1379,
            token,
            key,
            block_size,
            total_block,
            file_path,
            file_size,
            metrics=metrics,
            block_workers=block_workers
        )
        metrics['upload_seconds'] = time.perf_counter() - upload_start
        if not ok:
            print("UPLOAD failed: see logs for details")
            return None

        verify_start = time.perf_counter()
        server_md5, get_resp = verify_upload(sock, token, key, file_path)
        metrics['verify_seconds'] = time.perf_counter() - verify_start
        print(f"GET response: {json.dumps(get_resp, indent=2) if get_resp is not None else None}")
        if server_md5 is None:
            print(f"GET failed: {None if get_resp is None else get_resp.get('status_msg')}")
            return None

        local_md5 = get_file_md5(file_path)
        print(f"Local MD5:  {local_md5}")
        print(f"Server MD5: {server_md5}")
        if server_md5 == local_md5:
            print("Upload verified successfully!")
            logger.info('Upload verified successfully! MD5 match.')
        else:
            print("MD5 mismatch! Upload may be corrupted.")
            logger.error('MD5 mismatch! Upload may be corrupted.')

        logger.info('Client session ended.')
    metrics['total_seconds'] = time.perf_counter() - total_start
    if metrics.get('upload_seconds') and metrics['upload_seconds'] > 0:
        metrics['throughput_mbps'] = (
            metrics['file_size_bytes'] / metrics['upload_seconds'] / (1024 * 1024)
        )
    else:
        metrics['throughput_mbps'] = None
    return metrics


def main():
    args = _argparse()
    server_ip = args.server_ip
    student_id = args.id
    file_paths = []
    if args.files:
        file_paths.extend(args.files)
    if args.f:
        file_paths.append(args.f)

    # Remove duplicates while preserving order
    seen = set()
    unique_paths = []
    for path in file_paths:
        if path not in seen:
            unique_paths.append(path)
            seen.add(path)
    file_paths = unique_paths

    if not file_paths:
        logger.error('No files specified for upload.')
        print("No files specified.")
        return

    if len(file_paths) == 1:
        file_path = file_paths[0]
        logger.info(f'Starting client. Server: {server_ip}, ID: {student_id}, File: {file_path}')
        tcp_sender(server_ip, student_id, file_path, block_workers=args.block_workers)
        logger.info(f'Client finished.')
        return

    logger.info(f'Starting sequential multi-upload for {len(file_paths)} files.')
    print(f"Starting multi-upload: files={len(file_paths)}, block_workers={args.block_workers}")

    results = []
    for path in file_paths:
        logger.info(f'Uploading file: {path}')
        metrics = tcp_sender(server_ip, student_id, path, block_workers=args.block_workers)
        results.append({
            'file': path,
            'metrics': metrics
        })

    if results:
        print("\nSummary:")
        headers = ["File", "Size (MB)", "Upload Time (s)", "Throughput (MB/s)", "Status"]
        col_widths = [max(len(h), 20), 12, 16, 18, 10]

        def fmt_row(values):
            return " | ".join(str(v).ljust(w) for v, w in zip(values, col_widths))

        print(fmt_row(headers))
        print("-+-".join("-" * w for w in col_widths))
        for item in results:
            metrics = item['metrics']
            if metrics is None:
                status = "FAILED"
                size_mb = "—"
                upload_time = "—"
                throughput = "—"
            else:
                status = "OK"
                size_mb = f"{metrics['file_size_bytes'] / (1024 * 1024):.2f}"
                upload_time = f"{metrics['upload_seconds']:.3f}" if metrics.get('upload_seconds') else "—"
                throughput = f"{metrics['throughput_mbps']:.2f}" if metrics.get('throughput_mbps') else "—"
            print(fmt_row([
                item['file'],
                size_mb,
                upload_time,
                throughput,
                status
            ]))

    logger.info('Client finished (multi-upload).')


if __name__ == '__main__':
    main()