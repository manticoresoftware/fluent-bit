#!/usr/bin/env python3

import json
import os
import signal
import socket
import subprocess
import tempfile
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


def send_response(connection, body, status="200 OK"):
    response = (
        "HTTP/1.1 {}\r\n".format(status).encode()
        +
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


def capture_permanent_server_error(listener, result):
    connection, _ = listener.accept()
    result["request"] = read_request(connection)
    body = b'{"items":[{"insert":{"status":409}}],"errors":true}'
    send_response(connection, body, status="500 Internal Server Error")
    listener.close()


def stop_process(process, delay=2):
    time.sleep(delay)
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        output, _ = process.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        output, _ = process.communicate()
        raise AssertionError("Fluent Bit did not stop\n{}".format(output))
    return output


def assert_rejected_before_connect(fluent_bit, payload, expected, copies=None):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(0.5)
    port = listener.getsockname()[1]

    command = [
        fluent_bit,
        "-f", "0.2",
        "-i", "dummy",
        "-p", "dummy={}".format(json.dumps(payload, separators=(",", ":"))),
        "-p", "samples=1",
    ]
    if copies is not None:
        command.extend(["-p", "copies={}".format(copies)])
    command.extend([
        "-o", "manticore",
        "-p", "host=127.0.0.1",
        "-p", "port={}".format(port),
        "-p", "table=wire_logs",
        "-m", "*",
    ])

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    time.sleep(2)
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    output, _ = process.communicate(timeout=15)

    connected = False
    try:
        connection, _ = listener.accept()
        connection.close()
        connected = True
    except socket.timeout:
        pass
    listener.close()

    assert not connected
    assert expected in output
    assert "retry in" not in output


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

    fixture_dir = tempfile.TemporaryDirectory()
    input_path = os.path.join(fixture_dir.name, "records.json")
    parsers_path = os.path.join(fixture_dir.name, "parsers.conf")
    with open(input_path, "w") as stream:
        for document_id in range(1, 21):
            stream.write(json.dumps({
                "id": document_id,
                "message": "wire-test",
                "status": 200,
            }) + "\n")
    with open(parsers_path, "w") as stream:
        stream.write("[PARSER]\n    Name manticore_json\n    Format json\n")

    command = [
        fluent_bit,
        "-f", "0.2",
        "-R", parsers_path,
        "-i", "tail",
        "-p", "path={}".format(input_path),
        "-p", "read_from_head=true",
        "-p", "parser=manticore_json",
        "-o", "manticore",
        "-p", "host=127.0.0.1",
        "-p", "port={}".format(port),
        "-p", "table=wire logs",
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

    assert result["request_line"] == (
        "POST /bulk?bulk_import=wire%20logs HTTP/1.1"
    )
    assert headers.get("transfer-encoding") == "chunked"
    assert headers.get("connection") == "close"
    assert "content-length" not in headers
    assert len(chunks) > 1
    assert len(records) == 20
    assert records == [
        {
            "insert": {
                "table": "wire logs",
                "id": document_id,
                "doc": {"message": "wire-test", "status": 200},
            }
        }
        for document_id in range(1, 21)
    ]
    fixture_dir.cleanup()

    print("captured {} records in {} HTTP chunks".format(
        len(records), len(chunks)))

    rejected_records = [
        ({"id": {"invalid": True}, "message": "poison"}, None,
         "must be a unique, non-zero numeric ID"),
        ({"message": "missing ID"}, None,
         "must contain a non-zero numeric ID"),
        ({"id": 0, "message": "zero ID"}, None,
         "must be a unique, non-zero numeric ID"),
        ({"id": 44, "message": "duplicate ID"}, 2,
         "must be unique within a chunk"),
    ]
    for payload, copies, expected_error in rejected_records:
        assert_rejected_before_connect(
            fluent_bit, payload, expected_error, copies=copies)
    print("permanent ID errors were rejected before delivery")

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
        "-p", 'dummy={"id":"43","message":"retry-item"}',
        "-p", "samples=1",
        "-o", "manticore",
        "-p", "host=127.0.0.1",
        "-p", "port={}".format(retry_port),
        "-p", "table=wire_logs",
        "-p", "action=create",
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
    assert json.loads(first_body) == {
        "create": {
            "table": "wire_logs",
            "id": 43,
            "doc": {"message": "retry-item"},
        }
    }
    assert "retryable item error" in retry_output
    print("transient item error retried the original chunk")

    permanent_result = {}
    permanent_listener = socket.socket()
    permanent_listener.bind(("127.0.0.1", 0))
    permanent_listener.listen(1)
    permanent_port = permanent_listener.getsockname()[1]
    permanent_server = threading.Thread(
        target=capture_permanent_server_error,
        args=(permanent_listener, permanent_result),
        daemon=True,
    )
    permanent_server.start()
    permanent_command = [
        fluent_bit,
        "-f", "0.2",
        "-i", "dummy",
        "-p", 'dummy={"id":44,"message":"permanent-item"}',
        "-p", "samples=1",
        "-o", "manticore",
        "-p", "host=127.0.0.1",
        "-p", "port={}".format(permanent_port),
        "-p", "table=wire_logs",
        "-m", "*",
    ]
    permanent_proc = subprocess.Popen(
        permanent_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    permanent_output = stop_process(permanent_proc)
    permanent_server.join(timeout=2)
    assert not permanent_server.is_alive()
    assert permanent_result["request"][0] == (
        "POST /bulk?bulk_import=wire_logs HTTP/1.1")
    assert "returned HTTP 500" in permanent_output
    assert "retry in" not in permanent_output
    print("permanent item status inside HTTP 500 was not retried")

    invalid_action_command = [
        fluent_bit,
        "-f", "0.2",
        "-i", "dummy",
        "-p", "samples=1",
        "-o", "manticore",
        "-p", "host=127.0.0.1",
        "-p", "port=9",
        "-p", "table=wire_logs",
        "-p", "action=replace",
        "-m", "*",
    ]
    invalid_action_process = subprocess.run(
        invalid_action_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        timeout=15,
    )
    assert invalid_action_process.returncode != 0
    assert "action must be 'insert' or 'create'" in invalid_action_process.stdout
    print("replace action rejected before delivery")


if __name__ == "__main__":
    main()
