#!/usr/bin/env python3

import json
import os
import socket
import tempfile
import threading
import time

import pytest

from utils.fluent_bit_manager import FluentBitStartupError
from utils.test_service import FluentBitTestService


pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="Manticore single_chunk is not supported on Windows"
)


CONFIG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../config"))


def config_path(config_file):
    return os.path.join(CONFIG_DIR, config_file)


def config_environment(**values):
    return {key: str(value) for key, value in values.items()}


def managed_service(config_file, environment):
    return FluentBitTestService(config_path(config_file), extra_env=environment)


def read_service_log(service):
    with open(service.flb.log_file, "r", encoding="utf-8", errors="replace") as stream:
        return stream.read()


def stop_managed_service(service):
    process = service.flb.process
    log_file = service.flb.log_file
    service.stop()
    with open(log_file, "r", encoding="utf-8", errors="replace") as stream:
        output = stream.read()
    return process.returncode, output


def write_rejection_config(directory, payload, *, copies=None, output_options=None):
    output_options = output_options or {}
    input_path = os.path.join(directory, "records.json")
    parsers_path = os.path.join(directory, "parsers.conf")
    records = [payload] * (copies if copies is not None else 1)
    with open(input_path, "w", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record, separators=(",", ":")) + "\n")
    with open(parsers_path, "w", encoding="utf-8") as stream:
        stream.write("[PARSER]\n    Name rejection_json\n    Format json\n")

    lines = [
        "service:",
        "  flush: 0.2",
        "  grace: 3",
        "  log_level: info",
        "  http_server: on",
        "  http_port: ${FLUENT_BIT_HTTP_MONITORING_PORT}",
        "  parsers_file: ${MANTICORE_PARSERS_FILE}",
        "",
        "pipeline:",
        "  inputs:",
        "    - name: tail",
        "      tag: manticore_rejection",
        "      path: ${MANTICORE_INPUT_PATH}",
        "      read_from_head: true",
        "      exit_on_eof: true",
        "      parser: rejection_json",
    ]
    lines.extend([
        "",
        "  outputs:",
        "    - name: manticore",
        "      match: '*'",
        "      host: 127.0.0.1",
        "      port: ${MANTICORE_PORT}",
        "      table: wire_logs",
    ])
    for key, value in output_options.items():
        lines.append("      {}: {}".format(key, value))

    path = os.path.join(directory, "rejection.yaml")
    with open(path, "w", encoding="utf-8") as stream:
        stream.write("\n".join(lines) + "\n")
    return path, input_path, parsers_path


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


def wait_for_service_log(service, expected, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        output = read_service_log(service)
        if expected in output:
            return output
        time.sleep(0.1)
    raise AssertionError("Fluent Bit log did not contain {!r}\n{}".format(
        expected, read_service_log(service)))


def wait_for_path(path, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(path):
            return
        time.sleep(0.1)
    raise AssertionError("Path did not appear before timeout: {}".format(path))


def assert_rejected_before_connect(payload, expected, copies=None,
                                   output_options=None):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(0.5)
    port = listener.getsockname()[1]

    with tempfile.TemporaryDirectory(prefix="manticore-rejected-") as directory:
        config_file, input_path, parsers_path = write_rejection_config(
            directory, payload, copies=copies, output_options=output_options)
        environment = config_environment(
            MANTICORE_INPUT_PATH=input_path,
            MANTICORE_PARSERS_FILE=parsers_path,
            MANTICORE_PORT=port,
        )
        service = FluentBitTestService(config_file, extra_env=environment)
        with pytest.raises(FluentBitStartupError):
            service.start()
        output = read_service_log(service)

    connected = False
    try:
        connection, _ = listener.accept()
        connection.close()
        connected = True
    except socket.timeout:
        pass
    finally:
        listener.close()

    assert not connected
    assert expected in output
    assert "retry in" not in output


def test_out_manticore_chunked_and_recovery():
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

    environment = config_environment(
        MANTICORE_INPUT_PATH=input_path,
        MANTICORE_PARSERS_FILE=parsers_path,
        MANTICORE_PORT=port,
    )
    service = managed_service("out_manticore_wire.yaml", environment)
    service.start()

    server.join(20)
    returncode, output = stop_managed_service(service)

    if server.is_alive():
        raise AssertionError("no request received\n{}".format(output))
    if returncode != 0:
        raise AssertionError("Fluent Bit exited {}\n{}".format(
            returncode, output))

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

    session_result = {}
    session_listener = socket.socket()
    session_listener.bind(("127.0.0.1", 0))
    session_listener.listen(1)
    session_port = session_listener.getsockname()[1]
    session_server = threading.Thread(
        target=capture_request,
        args=(session_listener, session_result),
        daemon=True,
    )
    session_server.start()
    session_dir = tempfile.TemporaryDirectory()
    session_pattern = os.path.join(session_dir.name, "session-*.json")
    session_input = os.path.join(session_dir.name, "session-records.json")
    session_source = os.path.join(session_dir.name, "source.json")
    session_parser = os.path.join(session_dir.name, "parsers.conf")
    session_spool = os.path.join(session_dir.name, "session.ndjson")
    session_record_count = 3000 if os.environ.get("VALGRIND") else 30000
    with open(session_source, "w") as stream:
        for document_id in range(1, session_record_count + 1):
            stream.write(json.dumps({
                "id": document_id,
                "message": "session-test",
                "payload": "x" * 128,
            }, separators=(",", ":")) + "\n")
    with open(session_parser, "w") as stream:
        stream.write("[PARSER]\n    Name session_json\n    Format json\n")
    with open(session_source, "rb") as stream:
        session_data = stream.read()
    session_environment = config_environment(
        MANTICORE_INPUT_PATH=session_pattern,
        MANTICORE_PARSERS_FILE=session_parser,
        MANTICORE_PORT=session_port,
        MANTICORE_SPOOL_PATH=session_spool,
        MANTICORE_EXIT_ON_EOF="true",
    )
    session_service = managed_service(
        "out_manticore_session.yaml", session_environment)
    session_service.start()
    os.replace(session_source, session_input)
    session_timeout = 180 if os.environ.get("VALGRIND") else 60
    session_server.join(timeout=session_timeout)
    session_returncode, session_output = stop_managed_service(session_service)
    assert session_returncode == 0, session_output
    assert not session_server.is_alive()
    session_records = b"".join(session_result["chunks"]).splitlines()
    assert len(session_records) == session_record_count
    assert session_result["request_line"] == (
        "POST /bulk?bulk_import=session_logs HTTP/1.1")
    assert not os.path.exists(session_spool)
    assert not os.path.exists(session_spool + ".commit")
    assert "service has stopped (0 pending tasks)" in session_output

    rejected_listener = socket.socket()
    rejected_listener.bind(("127.0.0.1", 0))
    rejected_listener.listen(1)
    rejected_listener.settimeout(1)
    rejected_port = rejected_listener.getsockname()[1]
    rejected_pattern = os.path.join(session_dir.name, "rejected-*.json")
    rejected_input = os.path.join(session_dir.name, "rejected-records.json")
    rejected_data = session_data + json.dumps({
        "id": 1,
        "message": "duplicate-late",
        "payload": "x" * 128,
    }, separators=(",", ":")).encode() + b"\n"
    rejected_environment = config_environment(
        MANTICORE_INPUT_PATH=rejected_pattern,
        MANTICORE_PARSERS_FILE=session_parser,
        MANTICORE_PORT=rejected_port,
        MANTICORE_SPOOL_PATH=session_spool,
        MANTICORE_EXIT_ON_EOF="true",
    )
    rejected_service = managed_service(
        "out_manticore_session.yaml", rejected_environment)
    rejected_service.start()
    with open(rejected_input, "wb") as stream:
        stream.write(rejected_data)
    wait_for_service_log(rejected_service, "must be unique within a session")
    _, rejected_output = stop_managed_service(rejected_service)
    connected = False
    try:
        rejected_connection, _ = rejected_listener.accept()
        rejected_connection.close()
        connected = True
    except socket.timeout:
        pass
    rejected_listener.close()
    assert not connected
    assert "must be unique within a session" in rejected_output
    assert "single-chunk session aborted" in rejected_output
    assert not os.path.exists(session_spool)
    assert not os.path.exists(session_spool + ".commit")
    session_dir.cleanup()
    print("single_chunk combined 30000 records and aborted a late duplicate")

    recovery_dir = tempfile.TemporaryDirectory()
    recovery_input = os.path.join(recovery_dir.name, "recovery.json")
    recovery_empty = os.path.join(recovery_dir.name, "empty.json")
    recovery_parser = os.path.join(recovery_dir.name, "parsers.conf")
    recovery_spool = os.path.join(recovery_dir.name, "recovery.ndjson")
    with open(recovery_input, "w") as stream:
        stream.write('{"id":70001,"message":"recover-me"}\n')
    open(recovery_empty, "w").close()
    with open(recovery_input, "rb") as stream:
        recovery_data = stream.read()
    with open(recovery_parser, "w") as stream:
        stream.write("[PARSER]\n    Name recovery_json\n    Format json\n")
    unavailable = socket.socket()
    unavailable.bind(("127.0.0.1", 0))
    recovery_port = unavailable.getsockname()[1]
    unavailable.close()

    def recovery_environment(path, table="recovery_logs", port=recovery_port,
                            exit_on_eof="false"):
        return config_environment(
            MANTICORE_INPUT_PATH=path,
            MANTICORE_PARSERS_FILE=recovery_parser,
            MANTICORE_PORT=port,
            MANTICORE_SPOOL_PATH=recovery_spool,
            MANTICORE_TABLE=table,
            MANTICORE_EXIT_ON_EOF=exit_on_eof,
        )

    failed_service = managed_service(
        "out_manticore_recovery.yaml", recovery_environment(recovery_input))
    failed_service.start()
    wait_for_path(recovery_spool)
    failed_returncode, failed_output = stop_managed_service(failed_service)
    assert failed_returncode == 0
    assert os.path.getsize(recovery_spool) > 0
    assert os.path.getsize(recovery_spool + ".commit") > 0
    assert "preserving spool" in failed_output

    with open(recovery_spool, "ab") as stream:
        stream.write(b'{"insert":{"table":"recovery_logs","id":999')
    with open(recovery_spool + ".commit", "ab") as stream:
        stream.write(b"000000000000")

    recovery_result = {}
    recovery_listener = socket.socket()
    recovery_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recovery_listener.bind(("127.0.0.1", recovery_port))
    recovery_listener.listen(1)
    recovery_server = threading.Thread(
        target=capture_request,
        args=(recovery_listener, recovery_result),
        daemon=True,
    )
    recovery_server.start()
    open(recovery_empty, "w").close()
    recovered_service = managed_service(
        "out_manticore_recovery.yaml",
        recovery_environment(recovery_empty, exit_on_eof="false"))
    recovered_service.start()
    recovered_returncode, recovered_output = stop_managed_service(
        recovered_service)
    recovery_server.join(timeout=30)
    assert recovered_returncode == 0, recovered_output
    assert not recovery_server.is_alive()
    recovered_records = b"".join(recovery_result["chunks"]).splitlines()
    assert len(recovered_records) == 1
    assert json.loads(recovered_records[0])["insert"]["id"] == 70001
    assert "replaying pending single-chunk spool" in recovered_output
    assert not os.path.exists(recovery_spool)
    assert not os.path.exists(recovery_spool + ".commit")

    with open(recovery_input, "wb") as stream:
        stream.write(recovery_data)
    second_failed_service = managed_service(
        "out_manticore_recovery.yaml", recovery_environment(recovery_input))
    second_failed_service.start()
    wait_for_path(recovery_spool)
    second_failed_returncode, _ = stop_managed_service(second_failed_service)
    assert second_failed_returncode == 0
    with open(recovery_spool, "r+b") as stream:
        data = stream.read()
        marker = data.index(b"recover-me")
        stream.seek(marker)
        stream.write(b"Recover-me")
    corrupt_service = managed_service(
        "out_manticore_recovery.yaml",
        recovery_environment(recovery_empty, exit_on_eof="false"))
    open(recovery_empty, "w").close()
    with pytest.raises(FluentBitStartupError):
        corrupt_service.start()
    corrupt_output = read_service_log(corrupt_service)
    assert "commit journal or spool" in corrupt_output
    assert "is corrupt" in corrupt_output
    os.remove(recovery_spool)
    os.remove(recovery_spool + ".commit")

    with open(recovery_input, "wb") as stream:
        stream.write(recovery_data)
    third_failed_service = managed_service(
        "out_manticore_recovery.yaml", recovery_environment(recovery_input))
    third_failed_service.start()
    wait_for_path(recovery_spool)
    third_failed_returncode, _ = stop_managed_service(third_failed_service)
    assert third_failed_returncode == 0

    changed_config_result = {}
    changed_config_listener = socket.socket()
    changed_config_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    changed_config_listener.bind(("127.0.0.1", recovery_port))
    changed_config_listener.listen(1)
    changed_config_server = threading.Thread(
        target=capture_request,
        args=(changed_config_listener, changed_config_result),
        daemon=True,
    )
    changed_config_server.start()
    changed_config_environment = recovery_environment(
        recovery_empty, table="other_logs", exit_on_eof="false")
    changed_config_service = managed_service(
        "out_manticore_recovery.yaml", changed_config_environment)
    open(recovery_empty, "w").close()
    changed_config_service.start()
    changed_config_returncode, changed_config_output = stop_managed_service(
        changed_config_service)
    changed_config_server.join(timeout=30)
    assert changed_config_returncode == 0, changed_config_output
    assert not changed_config_server.is_alive()
    assert changed_config_result["request_line"] == \
        "POST /bulk?bulk_import=other_logs HTTP/1.1"
    recovery_dir.cleanup()
    print("recovery discarded torn tails and replayed under changed configuration")

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
        assert_rejected_before_connect(payload, expected_error, copies=copies)
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

    retry_environment = config_environment(MANTICORE_PORT=retry_port)
    retry_service = managed_service("out_manticore_retry.yaml", retry_environment)
    retry_service.start()
    retry_server.join(20)
    retry_returncode, retry_output = stop_managed_service(retry_service)

    if retry_server.is_alive():
        raise AssertionError("transient item was not retried\n{}".format(
            retry_output))

    assert retry_returncode == 0, retry_output
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
    permanent_environment = config_environment(MANTICORE_PORT=permanent_port)
    permanent_service = managed_service(
        "out_manticore_permanent.yaml", permanent_environment)
    permanent_service.start()
    permanent_server.join(timeout=20)
    permanent_returncode, permanent_output = stop_managed_service(
        permanent_service)
    assert permanent_returncode == 0, permanent_output
    assert not permanent_server.is_alive()
    assert permanent_result["request"][0] == (
        "POST /bulk?bulk_import=wire_logs HTTP/1.1")
    assert "returned HTTP 500" in permanent_output
    assert "retry in" not in permanent_output
    print("permanent item status inside HTTP 500 was not retried")

    assert_rejected_before_connect(
        {"id": 44, "message": "invalid action"},
        "action must be 'insert' or 'create'",
        output_options={"action": "replace"},
    )
    print("replace action rejected before delivery")

    for extra, expected in [
        ({"workers": "2"},
         "single_chunk requires exactly one output worker"),
        ({"action": "create"},
         "single_chunk requires action 'insert' for replay safety"),
        ({"max_session_ids": "0"},
         "stream_chunk_size and max_session_ids must be greater than zero"),
    ]:
        with tempfile.TemporaryDirectory(prefix="manticore-single-config-") as directory:
            output_options = {
                "workers": "1",
                "single_chunk": "true",
                "spool_path": os.path.join(directory, "spool.ndjson"),
            }
            output_options.update(extra)
            assert_rejected_before_connect(
                {"id": 44, "message": "invalid single_chunk option"},
                expected,
                output_options=output_options,
            )
    print("invalid single_chunk worker/action/limit configurations rejected")
