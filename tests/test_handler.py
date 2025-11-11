import base64
import io
import os
import sys
import types
import unittest
import zipfile
from unittest.mock import MagicMock, patch


def _ensure_stub_modules():
    """Inject lightweight stubs for optional dependencies used in handler.py."""
    if "requests" not in sys.modules:
        requests_module = types.ModuleType("requests")

        class _RequestException(Exception):
            pass

        class Timeout(_RequestException):
            pass

        requests_module.RequestException = _RequestException
        requests_module.Timeout = Timeout
        requests_module.get = lambda *args, **kwargs: None
        requests_module.post = lambda *args, **kwargs: None
        requests_module.codes = types.SimpleNamespace(ok=200)
        sys.modules["requests"] = requests_module

    if "websocket" not in sys.modules:
        websocket_module = types.ModuleType("websocket")

        class WebSocket:
            def connect(self, *args, **kwargs):
                raise NotImplementedError("websocket stub")

            def recv(self):
                raise NotImplementedError("websocket stub")

            def close(self):
                pass

        class WebSocketException(Exception):
            pass

        class WebSocketTimeoutException(WebSocketException):
            pass

        class WebSocketConnectionClosedException(WebSocketException):
            pass

        websocket_module.WebSocket = WebSocket
        websocket_module.WebSocketException = WebSocketException
        websocket_module.WebSocketTimeoutException = WebSocketTimeoutException
        websocket_module.WebSocketConnectionClosedException = (
            WebSocketConnectionClosedException
        )
        websocket_module.enableTrace = lambda *args, **kwargs: None
        sys.modules["websocket"] = websocket_module

    if "runpod" not in sys.modules:
        runpod_module = types.ModuleType("runpod")
        serverless_module = types.ModuleType("runpod.serverless")
        utils_module = types.ModuleType("runpod.serverless.utils")
        rp_upload_module = types.ModuleType("runpod.serverless.utils.rp_upload")

        def _dummy_upload_image(job_id, file_path):
            return f"s3://{job_id}/{os.path.basename(file_path)}"

        rp_upload_module.upload_image = _dummy_upload_image
        utils_module.rp_upload = rp_upload_module
        serverless_module.utils = utils_module
        serverless_module.start = lambda *args, **kwargs: None
        runpod_module.serverless = serverless_module

        sys.modules["runpod"] = runpod_module
        sys.modules["runpod.serverless"] = serverless_module
        sys.modules["runpod.serverless.utils"] = utils_module
        sys.modules["runpod.serverless.utils.rp_upload"] = rp_upload_module


_ensure_stub_modules()

import handler


class TestHandlerUtilities(unittest.TestCase):
    def test_validate_input_single_workflow(self):
        input_payload = {"workflow": {"foo": "bar"}}
        validated, error = handler.validate_input(input_payload)

        self.assertIsNone(error)
        self.assertTrue(validated["zip_outputs"])
        self.assertEqual(len(validated["jobs"]), 1)
        job = validated["jobs"][0]
        self.assertEqual(job["job_label"], "job-1")
        self.assertEqual(job["workflow"], {"foo": "bar"})
        self.assertEqual(job["images"], [])

    def test_validate_input_jobs_array(self):
        input_payload = {
            "zip_outputs": False,
            "zip_filename_prefix": "ali-batch",
            "jobs": [
                {
                    "job_label": "ali",
                    "workflow": {"foo": 1},
                    "images": [{"name": "a.png", "image": "data:image/png;base64,AAA=="}],
                },
                {
                    "workflow": {"bar": 2},
                },
            ],
        }

        validated, error = handler.validate_input(input_payload)
        self.assertIsNone(error)
        self.assertFalse(validated["zip_outputs"])
        self.assertEqual(validated["zip_filename_prefix"], "ali-batch")
        self.assertEqual(len(validated["jobs"]), 2)
        self.assertEqual(validated["jobs"][0]["job_label"], "ali")
        self.assertEqual(validated["jobs"][1]["job_label"], "job-2")

    def test_validate_input_invalid_images(self):
        input_payload = {"workflow": {"foo": "bar"}, "images": [{"name": "missing"}]}
        _, error = handler.validate_input(input_payload)
        self.assertIsNotNone(error)
        self.assertIn("images[1]", error)

    def test_create_zip_archive(self):
        payloads = [
            {"job_label": "ali", "filename": "vid1.mp4", "bytes": b"AAA"},
            {"job_label": "moe", "filename": "vid2.mp4", "bytes": b"BBB"},
        ]
        zip_entry = handler.create_zip_archive(payloads, zip_prefix="batch")
        self.assertIsNotNone(zip_entry)
        decoded = base64.b64decode(zip_entry["data"])
        with zipfile.ZipFile(io.BytesIO(decoded), "r") as archive:
            self.assertEqual(set(archive.namelist()), {"ali/vid1.mp4", "moe/vid2.mp4"})
            self.assertEqual(archive.read("ali/vid1.mp4"), b"AAA")

    @patch("handler.requests.get")
    def test_check_server_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp
        self.assertTrue(handler.check_server("http://127.0.0.1:8188", retries=1, delay=10))

    @patch("handler.requests.get")
    def test_check_server_failure(self, mock_get):
        mock_get.side_effect = handler.requests.RequestException("boom")
        self.assertFalse(handler.check_server("http://127.0.0.1:8188", retries=1, delay=10))


if __name__ == "__main__":
    unittest.main()
