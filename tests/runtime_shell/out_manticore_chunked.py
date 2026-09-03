#!/usr/bin/env python3

import json
import os
import signal
import socket
import subprocess
import threading
import time


def read_request(connection):
    stream = connection.makefile("rb")
    request_line = stream.readline().decode().strip()
    headers = {}

    while True:
        line = stream.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        key, value = line.decode().split(":", 1)
        headers[key.lower()] = value.strip()

    chunks = []
    while True:
        size_line = stream.readline().strip()
        size = int(size_line.split(b";", 1)[0], 16)
        if size == 0:
            stream.readline()
            break
        chunks.append(stream.read(size))
        if stream.read(2) != b"\r\n":
            raise AssertionError("invalid HTTP chunk terminator")

    return request_line, headers, chunks


def send_response(connection, body):
    response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"Connection: close\r\n\r\n" + body
    )
    connection.sendall(response)
    connection.close()


def capture_request(listener, result):
    connection, _ = listener.accept()
    request_line, headers, chunks = read_request(connection)
    result.update(
        request_line=request_line,
        headers=headers,
        chunks=chunks,
    )

    body = (
        b'{"items":[{"bulk":{"created":20,"status":201}}],'
        b'"current_line":20,"skipped_lines":0,"errors": false,"error":""}'
    )
    send_response(connection, body)
    listener.close()


def capture_retry(listener, result):
    responses = [
        b'{"items":[{"bulk":{"status":503}}],"errors":true}',
        b'{"items":[{"bulk":{"status":201}}],"errors":false}',
    ]
    result["requests"] = []

    for body in responses:
        connection, _ = listener.accept()
        result["requests"].append(read_request(connection))
        send_response(connection, body)

    listener.close()


def main():
    fluent_bit = os.environ["FLB_BIN"]
    result = {}
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    server = threading.Thread(
        target=capture_request,
        args=(listener, result),
        daemon=True,
    )
    server.start()

    command = [
        fluent_bit,
        "-f", "0.2",
        "-i", "dummy",
        "-p", 'dummy={"id":42,"message":"wire-test","status":200}',
        "-p", "samples=1",
        "-p", "copies=20",
        "-o", "manticore",
        "-p", "host=127.0.0.1",
        "-p", "port={}".format(port),
        "-p", "table=wire_logs",
        "-p", "stream_chunk_size=128",
        "-m", "*",
    ]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )

    server.join(20)
    time.sleep(0.2)
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)

    try:
        output, _ = process.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        output, _ = process.communicate()
        raise AssertionError("Fluent Bit did not stop\n{}".format(output))

    if server.is_alive():
        raise AssertionError("no request received\n{}".format(output))
    if process.returncode != 0:
        raise AssertionError("Fluent Bit exited {}\n{}".format(
            process.returncode, output))

    headers = result["headers"]
    chunks = result["chunks"]
    records = [json.loads(line) for line in b"".join(chunks).splitlines()]
    expected = {
        "replace": {
            "table": "wire_logs",
            "id": 42,
            "doc": {"message": "wire-test", "status": 200},
        }
    }

    assert result["request_line"] == "POST /bulk HTTP/1.1"
    assert headers.get("transfer-encoding") == "chunked"
    assert "content-length" not in headers
    assert len(chunks) > 1
    assert len(records) == 20
    assert all(record == expected for record in records)

    print("captured {} records in {} HTTP chunks".format(
        len(records), len(chunks)))

    poison_result = {"connections": 0}
    poison_listener = socket.socket()
    poison_listener.bind(("127.0.0.1", 0))
    poison_listener.listen(1)
    poison_listener.settimeout(0.5)
    poison_port = poison_listener.getsockname()[1]

    poison_command = [
        fluent_bit,
        "-f", "0.2",
        "-i", "dummy",
        "-p", 'dummy={"id":{"invalid":true},"message":"poison"}',
        "-p", "samples=1",
        "-o", "manticore",
        "-p", "host=127.0.0.1",
        "-p", "port={}".format(poison_port),
        "-p", "table=wire_logs",
        "-m", "*",
    ]
    poison_process = subprocess.Popen(
        poison_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    time.sleep(2)
    if poison_process.poll() is None:
        poison_process.send_signal(signal.SIGTERM)
    poison_output, _ = poison_process.communicate(timeout=15)
    try:
        connection, _ = poison_listener.accept()
        connection.close()
        poison_result["connections"] += 1
    except socket.timeout:
        pass
    poison_listener.close()

    assert poison_result["connections"] == 0
    assert "must be an integer or string" in poison_output
    assert "retry in" not in poison_output
    print("permanent record error was not retried")

    retry_result = {}
    retry_listener = socket.socket()
    retry_listener.bind(("127.0.0.1", 0))
    retry_listener.listen(2)
    retry_port = retry_listener.getsockname()[1]
    retry_server = threading.Thread(
        target=capture_retry,
        args=(retry_listener, retry_result),
        daemon=True,
    )
    retry_server.start()

    retry_command = [
        fluent_bit,
        "-f", "0.2",
        "-i", "dummy",
        "-p", 'dummy={"id":43,"message":"retry-item"}',
        "-p", "samples=1",
        "-o", "manticore",
        "-p", "host=127.0.0.1",
        "-p", "port={}".format(retry_port),
        "-p", "table=wire_logs",
        "-m", "*",
    ]
    retry_process = subprocess.Popen(
        retry_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    retry_server.join(20)
    if retry_process.poll() is None:
        retry_process.send_signal(signal.SIGTERM)
    retry_output, _ = retry_process.communicate(timeout=15)

    if retry_server.is_alive():
        retry_process.kill()
        raise AssertionError("transient item was not retried\n{}".format(
            retry_output))

    assert len(retry_result["requests"]) == 2
    first_body = b"".join(retry_result["requests"][0][2])
    second_body = b"".join(retry_result["requests"][1][2])
    assert first_body == second_body
    assert "retryable item error" in retry_output
    print("transient item error retried the original chunk")


if __name__ == "__main__":
    main()
