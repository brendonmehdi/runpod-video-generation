import runpod
from runpod.serverless.utils import rp_upload
import json
import urllib.request
import urllib.parse
import time
import os
import requests
import base64
from io import BytesIO
import websocket
import uuid
import tempfile
import socket
import traceback
import zipfile
from pathlib import Path
import copy
import shutil

# Time to wait between API check attempts in milliseconds
COMFY_API_AVAILABLE_INTERVAL_MS = 50
# Maximum number of API check attempts
COMFY_API_AVAILABLE_MAX_RETRIES = 500
# Websocket reconnection behaviour (can be overridden through environment variables)
# NOTE: more attempts and diagnostics improve debuggability whenever ComfyUI crashes mid-job.
#   • WEBSOCKET_RECONNECT_ATTEMPTS sets how many times we will try to reconnect.
#   • WEBSOCKET_RECONNECT_DELAY_S sets the sleep in seconds between attempts.
#
# If the respective env-vars are not supplied we fall back to sensible defaults ("5" and "3").
WEBSOCKET_RECONNECT_ATTEMPTS = int(os.environ.get("WEBSOCKET_RECONNECT_ATTEMPTS", 5))
WEBSOCKET_RECONNECT_DELAY_S = int(os.environ.get("WEBSOCKET_RECONNECT_DELAY_S", 3))

# Extra verbose websocket trace logs (set WEBSOCKET_TRACE=true to enable)
if os.environ.get("WEBSOCKET_TRACE", "false").lower() == "true":
    # This prints low-level frame information to stdout which is invaluable for diagnosing
    # protocol errors but can be noisy in production – therefore gated behind an env-var.
    websocket.enableTrace(True)

# Host where ComfyUI is running
COMFY_HOST = "127.0.0.1:8188"
# Enforce a clean state after each job is done
# see https://docs.runpod.io/docs/handler-additional-controls#refresh-worker
REFRESH_WORKER = os.environ.get("REFRESH_WORKER", "false").lower() == "true"

# ---------------------------------------------------------------------------
# Workflow/mode configuration
# ---------------------------------------------------------------------------
MODE_IMG2VIDEO = "img2video"
MODE_REF_VIDEO = "ref_video_lora"
DEFAULT_LORA_NAME = None  # Optional; only apply when caller supplies a LORA.
WORKFLOWS_DIR = Path(__file__).parent / "test_resources" / "workflows"
REF_VIDEO_WORKFLOW_PATH = WORKFLOWS_DIR / "wholebodyReplacer.json"
# Where ComfyUI reads uploaded assets; defaults to standard ComfyUI input dir.
COMFY_INPUT_DIR = Path(os.environ.get("COMFY_INPUT_DIR", "/workspace/ComfyUI/input"))

# ---------------------------------------------------------------------------
# Helper: quick reachability probe of ComfyUI HTTP endpoint (port 8188)
# ---------------------------------------------------------------------------


def _comfy_server_status():
    """Return a dictionary with basic reachability info for the ComfyUI HTTP server."""
    try:
        resp = requests.get(f"http://{COMFY_HOST}/", timeout=5)
        return {
            "reachable": resp.status_code == 200,
            "status_code": resp.status_code,
        }
    except Exception as exc:
        return {"reachable": False, "error": str(exc)}


def _attempt_websocket_reconnect(ws_url, max_attempts, delay_s, initial_error):
    """
    Attempts to reconnect to the WebSocket server after a disconnect.

    Args:
        ws_url (str): The WebSocket URL (including client_id).
        max_attempts (int): Maximum number of reconnection attempts.
        delay_s (int): Delay in seconds between attempts.
        initial_error (Exception): The error that triggered the reconnect attempt.

    Returns:
        websocket.WebSocket: The newly connected WebSocket object.

    Raises:
        websocket.WebSocketConnectionClosedException: If reconnection fails after all attempts.
    """
    print(
        f"worker-comfyui - Websocket connection closed unexpectedly: {initial_error}. Attempting to reconnect..."
    )
    last_reconnect_error = initial_error
    for attempt in range(max_attempts):
        # Log current server status before each reconnect attempt so that we can
        # see whether ComfyUI is still alive (HTTP port 8188 responding) even if
        # the websocket dropped. This is extremely useful to differentiate
        # between a network glitch and an outright ComfyUI crash/OOM-kill.
        srv_status = _comfy_server_status()
        if not srv_status["reachable"]:
            # If ComfyUI itself is down there is no point in retrying the websocket –
            # bail out immediately so the caller gets a clear "ComfyUI crashed" error.
            print(
                f"worker-comfyui - ComfyUI HTTP unreachable – aborting websocket reconnect: {srv_status.get('error', 'status '+str(srv_status.get('status_code')))}"
            )
            raise websocket.WebSocketConnectionClosedException(
                "ComfyUI HTTP unreachable during websocket reconnect"
            )

        # Otherwise we proceed with reconnect attempts while server is up
        print(
            f"worker-comfyui - Reconnect attempt {attempt + 1}/{max_attempts}... (ComfyUI HTTP reachable, status {srv_status.get('status_code')})"
        )
        try:
            # Need to create a new socket object for reconnect
            new_ws = websocket.WebSocket()
            new_ws.connect(ws_url, timeout=10)  # Use existing ws_url
            print(f"worker-comfyui - Websocket reconnected successfully.")
            return new_ws  # Return the new connected socket
        except (
            websocket.WebSocketException,
            ConnectionRefusedError,
            socket.timeout,
            OSError,
        ) as reconn_err:
            last_reconnect_error = reconn_err
            print(
                f"worker-comfyui - Reconnect attempt {attempt + 1} failed: {reconn_err}"
            )
            if attempt < max_attempts - 1:
                print(
                    f"worker-comfyui - Waiting {delay_s} seconds before next attempt..."
                )
                time.sleep(delay_s)
            else:
                print(f"worker-comfyui - Max reconnection attempts reached.")

    # If loop completes without returning, raise an exception
    print("worker-comfyui - Failed to reconnect websocket after connection closed.")
    raise websocket.WebSocketConnectionClosedException(
        f"Connection closed and failed to reconnect. Last error: {last_reconnect_error}"
    )


def _validate_images_field(images, context_label="input"):
    """Shared validation helper for image upload payloads."""
    if images is None:
        return [], None

    if not isinstance(images, list):
        return (
            None,
            f"'images' in {context_label} must be a list of objects with 'name' and 'image' keys",
        )

    for idx, image in enumerate(images, start=1):
        if not isinstance(image, dict) or "name" not in image or "image" not in image:
            return (
                None,
                f"'images[{idx}]' in {context_label} must contain 'name' and 'image' keys",
            )

    return images, None


def validate_input(job_input):
    """
    Validates the input for the handler function.

    Args:
        job_input (dict): The input data to validate.

    Returns:
        tuple: A tuple containing the validated data and an error message, if any.
               The structure is (validated_data, error_message).
    """
    if job_input is None:
        return None, "Please provide input"

    if isinstance(job_input, str):
        try:
            job_input = json.loads(job_input)
        except json.JSONDecodeError:
            return None, "Invalid JSON format in input"

    validated_jobs = []
    jobs_payload = job_input.get("jobs")
    global_mode = job_input.get("mode")

    def _append_job_from_definition(job_definition, job_index_label):
        if not isinstance(job_definition, dict):
            return f"'{job_index_label}' must be an object"

        workflow = job_definition.get("workflow")
        if workflow is not None:
            images, image_error = _validate_images_field(
                job_definition.get("images"), job_index_label
            )
            if image_error:
                return image_error

            job_label = (
                job_definition.get("job_label")
                or job_definition.get("label")
                or job_definition.get("id")
                or job_index_label
            )

            validated_jobs.append(
                {
                    "job_label": job_label,
                    "workflow": workflow,
                    "images": images,
                    "mode": job_definition.get("mode") or global_mode,
                }
            )
            return None

        # New: build workflows based on declared mode.
        mode = (job_definition.get("mode") or global_mode or "").strip().lower()
        if not mode:
            return f"Missing 'workflow' or 'mode' in {job_index_label}"

        if mode not in (MODE_IMG2VIDEO, MODE_REF_VIDEO):
            return f"Unsupported mode '{mode}' in {job_index_label}"

        if mode == MODE_REF_VIDEO:
            if "character_image" not in job_definition:
                return f"'character_image' is required in {job_index_label} for mode '{MODE_REF_VIDEO}'"
            if "reference_video" not in job_definition:
                return f"'reference_video' is required in {job_index_label} for mode '{MODE_REF_VIDEO}'"
            prompt = job_definition.get("prompt")
            if prompt is None:
                return f"'prompt' is required in {job_index_label} for mode '{MODE_REF_VIDEO}'"

            job_label = (
                job_definition.get("job_label")
                or job_definition.get("label")
                or job_definition.get("id")
                or job_index_label
            )

            validated_jobs.append(
                {
                    "job_label": job_label,
                    "mode": MODE_REF_VIDEO,
                    "prompt": prompt,
                    "neg_prompt": job_definition.get("neg_prompt"),
                    "character_image": job_definition.get("character_image"),
                    "character_image_name": job_definition.get("character_image_name"),
                    "reference_video": job_definition.get("reference_video"),
                    "reference_video_name": job_definition.get("reference_video_name"),
                    "lora_name": job_definition.get("lora_name"),
                    "use_face_crop": bool(job_definition.get("use_face_crop", False)),
                    "width": job_definition.get("width"),
                    "height": job_definition.get("height"),
                    "video_length": job_definition.get("video_length"),
                }
            )
            return None

        return (
            f"{job_index_label} in mode '{MODE_IMG2VIDEO}' must include a 'workflow' "
            "payload to preserve legacy behaviour."
        )

    if jobs_payload is not None:
        if not isinstance(jobs_payload, list) or len(jobs_payload) == 0:
            return None, "'jobs' must be a non-empty list of workflow definitions"

        for idx, job_definition in enumerate(jobs_payload, start=1):
            err = _append_job_from_definition(job_definition, f"jobs[{idx}]")
            if err:
                return None, err
    else:
        err = _append_job_from_definition(job_input, "job-1")
        if err:
            return None, err

    return (
        {
            "jobs": validated_jobs,
            "zip_outputs": bool(job_input.get("zip_outputs", True)),
            "zip_filename_prefix": job_input.get("zip_filename_prefix")
            or job_input.get("zip_prefix"),
            "mode": job_input.get("mode"),
        },
        None,
    )


# ---------------------------------------------------------------------------
# Workflow assembly helpers for the new reference-video mode
# ---------------------------------------------------------------------------


def _decode_data_uri(data_uri):
    """Decode a base64 data URI or plain base64 string into bytes."""
    if not isinstance(data_uri, str):
        raise ValueError("Expected base64 string for media payload.")
    if "," in data_uri:
        _, encoded = data_uri.split(",", 1)
    else:
        encoded = data_uri
    return base64.b64decode(encoded)


def _persist_input_file(data_uri_or_path, preferred_name, kind):
    """
    Persist an incoming base64 (or existing path) to the ComfyUI input directory.
    Returns the filename (not the full path) that the workflow should reference.
    """
    COMFY_INPUT_DIR.mkdir(parents=True, exist_ok=True)

    if isinstance(data_uri_or_path, str) and os.path.isfile(data_uri_or_path):
        file_path = Path(data_uri_or_path)
        target_name = preferred_name or file_path.name
        target_path = COMFY_INPUT_DIR / target_name
        shutil.copy(file_path, target_path)
        return target_name

    # Assume base64 content
    extension = "png" if kind == "image" else "mp4"
    filename = preferred_name or f"{kind}-{uuid.uuid4().hex}.{extension}"
    try:
        blob = _decode_data_uri(data_uri_or_path)
    except Exception as exc:
        raise ValueError(f"Failed to decode {kind} payload: {exc}") from exc

    target_path = COMFY_INPUT_DIR / filename
    with open(target_path, "wb") as fh:
        fh.write(blob)
    return filename


def _load_workflow_template(path: Path):
    if not path.exists():
        raise FileNotFoundError(
            f"Workflow template not found at {path}. "
            "Place wholebodyReplacer.json there or update REF_VIDEO_WORKFLOW_PATH."
        )
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _get_node_by_id(nodes, node_id):
    for node in nodes:
        if node.get("id") == node_id:
            return node
    return None


def _get_widget_value(nodes, node_id, index=0, key=None, fallback=None):
    node = _get_node_by_id(nodes, node_id)
    if not node:
        return fallback
    widgets = node.get("widgets_values")
    if isinstance(widgets, dict):
        return widgets.get(key, fallback)
    if isinstance(widgets, list):
        try:
            return widgets[index]
        except IndexError:
            return fallback
    return fallback


def _set_widget_value(nodes, node_id, value, index=0, key=None):
    node = _get_node_by_id(nodes, node_id)
    if not node:
        raise ValueError(f"Node {node_id} not found in workflow.")
    widgets = node.get("widgets_values")
    if isinstance(widgets, dict):
        if key is None:
            raise ValueError(f"Node {node_id} expects dict widget, provide key.")
        widgets[key] = value
    elif isinstance(widgets, list):
        while len(widgets) <= index:
            widgets.append(None)
        widgets[index] = value
    else:
        node["widgets_values"] = [value]


def _toggle_face_crop(workflow_dict, use_face_crop):
    """
    When face crop is disabled, rewire Set_reference_image (node 378) to use the
    original uploaded image (node 311) instead of the ImageCropByMaskAndResize (432).
    """
    if use_face_crop:
        return  # leave as-is

    nodes = workflow_dict.get("nodes", [])
    links = workflow_dict.get("links", [])
    last_link_id = workflow_dict.get("last_link_id", 0)

    set_ref_node = _get_node_by_id(nodes, 378)
    source_node = _get_node_by_id(nodes, 311)
    crop_node = _get_node_by_id(nodes, 432)

    if not set_ref_node or not source_node:
        return

    # Remove old link from crop node to set node if present
    old_link_id = None
    if set_ref_node.get("inputs"):
        old_link_id = set_ref_node["inputs"][0].get("link")
    if crop_node and crop_node.get("outputs"):
        for output in crop_node["outputs"]:
            if output.get("links") and old_link_id in output["links"]:
                output["links"] = [l for l in output["links"] if l != old_link_id]

    new_link_id = last_link_id + 1
    # Update target input
    if set_ref_node.get("inputs"):
        set_ref_node["inputs"][0]["link"] = new_link_id
    # Update source output links
    if source_node.get("outputs"):
        source_node["outputs"][0]["links"] = list(
            set(source_node["outputs"][0].get("links", []) + [new_link_id])
        )
    # Add new link entry
    links.append([new_link_id, 311, 0, 378, 0, "IMAGE"])
    workflow_dict["last_link_id"] = new_link_id


def build_ref_video_workflow(job_payload):
    """
    Build a workflow for the reference-video mode by patching the provided template.
    """
    template = _load_workflow_template(REF_VIDEO_WORKFLOW_PATH)
    workflow = copy.deepcopy(template)
    nodes = workflow.get("nodes", [])

    print(
        f"worker-comfyui - Building reference-video workflow from {REF_VIDEO_WORKFLOW_PATH}"
    )

    # Persist assets so ComfyUI can read them from disk.
    character_image_name = _persist_input_file(
        job_payload["character_image"],
        job_payload.get("character_image_name"),
        "image",
    )
    reference_video_name = _persist_input_file(
        job_payload["reference_video"],
        job_payload.get("reference_video_name"),
        "video",
    )
    print(
        f"worker-comfyui - Stored inputs for job '{job_payload.get('job_label')}' "
        f"(image={character_image_name}, video={reference_video_name})"
    )

    # Defaults pulled from the template in case caller omits numerics.
    width = job_payload.get("width") or _get_widget_value(nodes, 330, 0, fallback=720)
    height = job_payload.get("height") or _get_widget_value(nodes, 331, 0, fallback=1280)
    video_length = job_payload.get("video_length") or _get_widget_value(
        nodes, 383, 0, fallback=254
    )

    # Patch primary inputs.
    _set_widget_value(nodes, 311, character_image_name, index=0)  # LoadImage
    _set_widget_value(
        nodes, 417, reference_video_name, key="video"
    )  # VHS_LoadVideo expects filename
    _set_widget_value(nodes, 330, int(width), index=0)  # Video Width
    _set_widget_value(nodes, 331, int(height), index=0)  # Video Height
    _set_widget_value(nodes, 383, int(video_length), index=0)  # Video Length (frames)

    prompt = job_payload.get("prompt")
    neg_prompt = job_payload.get("neg_prompt")
    if prompt is not None:
        _set_widget_value(nodes, 227, prompt, index=0)  # Positive Prompt
    if neg_prompt is not None:
        _set_widget_value(nodes, 228, neg_prompt, index=0)  # Negative Prompt

    lora_name = job_payload.get("lora_name") or DEFAULT_LORA_NAME
    if lora_name:
        _set_widget_value(nodes, 464, lora_name, index=0)  # filename
        _set_widget_value(nodes, 464, 1, index=1)  # strength, keep default if present
    else:
        # Clear LORA filename and set strength to 0 so the node becomes a no-op when caller does not provide one.
        _set_widget_value(nodes, 464, "", index=0)
        _set_widget_value(nodes, 464, 0, index=1)

    _toggle_face_crop(workflow, job_payload.get("use_face_crop", False))

    print(
        f"worker-comfyui - Workflow patched for job '{job_payload.get('job_label')}' "
        f"(mode={MODE_REF_VIDEO}, face_crop={job_payload.get('use_face_crop', False)}, lora={lora_name})"
    )
    return workflow


def check_server(url, retries=500, delay=50):
    """
    Check if a server is reachable via HTTP GET request

    Args:
    - url (str): The URL to check
    - retries (int, optional): The number of times to attempt connecting to the server. Default is 50
    - delay (int, optional): The time in milliseconds to wait between retries. Default is 500

    Returns:
    bool: True if the server is reachable within the given number of retries, otherwise False
    """

    print(f"worker-comfyui - Checking API server at {url}...")
    for i in range(retries):
        try:
            response = requests.get(url, timeout=5)

            # If the response status code is 200, the server is up and running
            if response.status_code == 200:
                print(f"worker-comfyui - API is reachable")
                return True
        except requests.Timeout:
            pass
        except requests.RequestException as e:
            pass

        # Wait for the specified delay before retrying
        time.sleep(delay / 1000)

    print(
        f"worker-comfyui - Failed to connect to server at {url} after {retries} attempts."
    )
    return False


def upload_images(images):
    """
    Upload a list of base64 encoded images to the ComfyUI server using the /upload/image endpoint.

    Args:
        images (list): A list of dictionaries, each containing the 'name' of the image and the 'image' as a base64 encoded string.

    Returns:
        dict: A dictionary indicating success or error.
    """
    if not images:
        return {"status": "success", "message": "No images to upload", "details": []}

    responses = []
    upload_errors = []

    print(f"worker-comfyui - Uploading {len(images)} image(s)...")

    for image in images:
        try:
            name = image["name"]
            image_data_uri = image["image"]  # Get the full string (might have prefix)

            # --- Strip Data URI prefix if present ---
            if "," in image_data_uri:
                # Find the comma and take everything after it
                base64_data = image_data_uri.split(",", 1)[1]
            else:
                # Assume it's already pure base64
                base64_data = image_data_uri
            # --- End strip ---

            blob = base64.b64decode(base64_data)  # Decode the cleaned data

            # Prepare the form data
            files = {
                "image": (name, BytesIO(blob), "image/png"),
                "overwrite": (None, "true"),
            }

            # POST request to upload the image
            response = requests.post(
                f"http://{COMFY_HOST}/upload/image", files=files, timeout=30
            )
            response.raise_for_status()

            responses.append(f"Successfully uploaded {name}")
            print(f"worker-comfyui - Successfully uploaded {name}")

        except base64.binascii.Error as e:
            error_msg = f"Error decoding base64 for {image.get('name', 'unknown')}: {e}"
            print(f"worker-comfyui - {error_msg}")
            upload_errors.append(error_msg)
        except requests.Timeout:
            error_msg = f"Timeout uploading {image.get('name', 'unknown')}"
            print(f"worker-comfyui - {error_msg}")
            upload_errors.append(error_msg)
        except requests.RequestException as e:
            error_msg = f"Error uploading {image.get('name', 'unknown')}: {e}"
            print(f"worker-comfyui - {error_msg}")
            upload_errors.append(error_msg)
        except Exception as e:
            error_msg = (
                f"Unexpected error uploading {image.get('name', 'unknown')}: {e}"
            )
            print(f"worker-comfyui - {error_msg}")
            upload_errors.append(error_msg)

    if upload_errors:
        print(f"worker-comfyui - image(s) upload finished with errors")
        return {
            "status": "error",
            "message": "Some images failed to upload",
            "details": upload_errors,
        }

    print(f"worker-comfyui - image(s) upload complete")
    return {
        "status": "success",
        "message": "All images uploaded successfully",
        "details": responses,
    }


def get_available_models():
    """
    Get list of available models from ComfyUI

    Returns:
        dict: Dictionary containing available models by type
    """
    try:
        response = requests.get(f"http://{COMFY_HOST}/object_info", timeout=10)
        response.raise_for_status()
        object_info = response.json()

        # Extract available checkpoints from CheckpointLoaderSimple
        available_models = {}
        if "CheckpointLoaderSimple" in object_info:
            checkpoint_info = object_info["CheckpointLoaderSimple"]
            if "input" in checkpoint_info and "required" in checkpoint_info["input"]:
                ckpt_options = checkpoint_info["input"]["required"].get("ckpt_name")
                if ckpt_options and len(ckpt_options) > 0:
                    available_models["checkpoints"] = (
                        ckpt_options[0] if isinstance(ckpt_options[0], list) else []
                    )

        return available_models
    except Exception as e:
        print(f"worker-comfyui - Warning: Could not fetch available models: {e}")
        return {}


def queue_workflow(workflow, client_id):
    """
    Queue a workflow to be processed by ComfyUI

    Args:
        workflow (dict): A dictionary containing the workflow to be processed
        client_id (str): The client ID for the websocket connection

    Returns:
        dict: The JSON response from ComfyUI after processing the workflow

    Raises:
        ValueError: If the workflow validation fails with detailed error information
    """
    # Include client_id in the prompt payload
    payload = {"prompt": workflow, "client_id": client_id}
    data = json.dumps(payload).encode("utf-8")

    # Use requests for consistency and timeout
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        f"http://{COMFY_HOST}/prompt", data=data, headers=headers, timeout=30
    )

    # Handle validation errors with detailed information
    if response.status_code == 400:
        print(f"worker-comfyui - ComfyUI returned 400. Response body: {response.text}")
        try:
            error_data = response.json()
            print(f"worker-comfyui - Parsed error data: {error_data}")

            # Try to extract meaningful error information
            error_message = "Workflow validation failed"
            error_details = []

            # ComfyUI seems to return different error formats, let's handle them all
            if "error" in error_data:
                error_info = error_data["error"]
                if isinstance(error_info, dict):
                    error_message = error_info.get("message", error_message)
                    if error_info.get("type") == "prompt_outputs_failed_validation":
                        error_message = "Workflow validation failed"
                else:
                    error_message = str(error_info)

            # Check for node validation errors in the response
            if "node_errors" in error_data:
                for node_id, node_error in error_data["node_errors"].items():
                    if isinstance(node_error, dict):
                        for error_type, error_msg in node_error.items():
                            error_details.append(
                                f"Node {node_id} ({error_type}): {error_msg}"
                            )
                    else:
                        error_details.append(f"Node {node_id}: {node_error}")

            # Check if the error data itself contains validation info
            if error_data.get("type") == "prompt_outputs_failed_validation":
                error_message = error_data.get("message", "Workflow validation failed")
                # For this type of error, we need to parse the validation details from logs
                # Since ComfyUI doesn't seem to include detailed validation errors in the response
                # Let's provide a more helpful generic message
                available_models = get_available_models()
                if available_models.get("checkpoints"):
                    error_message += f"\n\nThis usually means a required model or parameter is not available."
                    error_message += f"\nAvailable checkpoint models: {', '.join(available_models['checkpoints'])}"
                else:
                    error_message += "\n\nThis usually means a required model or parameter is not available."
                    error_message += "\nNo checkpoint models appear to be available. Please check your model installation."

                raise ValueError(error_message)

            # If we have specific validation errors, format them nicely
            if error_details:
                detailed_message = f"{error_message}:\n" + "\n".join(
                    f"• {detail}" for detail in error_details
                )

                # Try to provide helpful suggestions for common errors
                if any(
                    "not in list" in detail and "ckpt_name" in detail
                    for detail in error_details
                ):
                    available_models = get_available_models()
                    if available_models.get("checkpoints"):
                        detailed_message += f"\n\nAvailable checkpoint models: {', '.join(available_models['checkpoints'])}"
                    else:
                        detailed_message += "\n\nNo checkpoint models appear to be available. Please check your model installation."

                raise ValueError(detailed_message)
            else:
                # Fallback to the raw response if we can't parse specific errors
                raise ValueError(f"{error_message}. Raw response: {response.text}")

        except (json.JSONDecodeError, KeyError) as e:
            # If we can't parse the error response, fall back to the raw text
            raise ValueError(
                f"ComfyUI validation failed (could not parse error response): {response.text}"
            )

    # For other HTTP errors, raise them normally
    response.raise_for_status()
    return response.json()


def get_history(prompt_id):
    """
    Retrieve the history of a given prompt using its ID

    Args:
        prompt_id (str): The ID of the prompt whose history is to be retrieved

    Returns:
        dict: The history of the prompt, containing all the processing steps and results
    """
    # Use requests for consistency and timeout
    response = requests.get(f"http://{COMFY_HOST}/history/{prompt_id}", timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_output_file(filename, subfolder, resource_type):
    """
    Fetch binary data (image/video) from the ComfyUI /view endpoint.

    Args:
        filename (str): Target filename produced by ComfyUI.
        subfolder (str): Subfolder for the artifact.
        resource_type (str): The type reported by ComfyUI (e.g., 'output', 'temp').

    Returns:
        bytes: Raw file contents or None if retrieval fails.
    """
    print(
        f"worker-comfyui - Fetching artifact: type={resource_type}, subfolder={subfolder}, filename={filename}"
    )
    data = {"filename": filename, "subfolder": subfolder, "type": resource_type}
    url_values = urllib.parse.urlencode(data)
    try:
        response = requests.get(f"http://{COMFY_HOST}/view?{url_values}", timeout=120)
        response.raise_for_status()
        print(f"worker-comfyui - Successfully fetched artifact for {filename}")
        return response.content
    except requests.Timeout:
        print(f"worker-comfyui - Timeout fetching artifact for {filename}")
        return None
    except requests.RequestException as e:
        print(f"worker-comfyui - Error fetching artifact for {filename}: {e}")
        return None
    except Exception as e:
        print(
            f"worker-comfyui - Unexpected error fetching artifact for {filename}: {e}"
        )
        return None


def create_zip_archive(binary_outputs, zip_prefix=None):
    """
    Bundle collected artifacts into a single base64-encoded ZIP archive.

    Args:
        binary_outputs (list): List of dictionaries with keys job_label, filename, bytes.
        zip_prefix (str): Optional prefix for the resulting zip filename.

    Returns:
        dict | None: Dictionary describing the zip payload or None if no data.
    """
    if not binary_outputs:
        return None

    archive_name = (zip_prefix or "outputs").replace(" ", "_")
    zip_filename = f"{archive_name}-{uuid.uuid4().hex[:8]}.zip"

    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for entry in binary_outputs:
            job_label = entry.get("job_label") or "job"
            internal_path = os.path.join(job_label, entry["filename"])
            zipf.writestr(internal_path, entry["bytes"])

    encoded_zip = base64.b64encode(buffer.getvalue()).decode("utf-8")
    return {"filename": zip_filename, "type": "base64", "data": encoded_zip}


def execute_workflow_job(runpod_job_id, job_label, workflow, input_images):
    """
    Execute a single ComfyUI workflow for an individual batch and collect outputs.
    """
    print(f"worker-comfyui - Starting workflow job '{job_label}'")
    ws = None
    client_id = str(uuid.uuid4())
    prompt_id = None
    output_data = []
    binary_outputs = []
    errors = []

    def _record_binary_output(filename, media_kind, file_bytes):
        binary_outputs.append(
            {
                "job_label": job_label,
                "media_kind": media_kind,
                "filename": filename,
                "bytes": file_bytes,
            }
        )

    def _store_media_payload(filename, media_kind, file_bytes):
        file_extension = os.path.splitext(filename)[1]
        if not file_extension:
            file_extension = ".mp4" if media_kind == "video" else ".png"
            filename = f"{filename}{file_extension}"

        safe_filename = os.path.basename(filename)
        _record_binary_output(safe_filename, media_kind, file_bytes)

        if os.environ.get("BUCKET_ENDPOINT_URL"):
            try:
                temp_file_path = None
                try:
                    with tempfile.NamedTemporaryFile(
                        suffix=file_extension, delete=False
                    ) as temp_file:
                        temp_file.write(file_bytes)
                        temp_file_path = temp_file.name
                    print(
                        f"worker-comfyui - Uploading {safe_filename} (job '{job_label}') to S3..."
                    )
                    s3_url = rp_upload.upload_image(runpod_job_id, temp_file_path)
                    output_data.append(
                        {
                            "filename": safe_filename,
                            "type": "s3_url",
                            "media_kind": media_kind,
                            "data": s3_url,
                        }
                    )
                finally:
                    if temp_file_path and os.path.exists(temp_file_path):
                        try:
                            os.remove(temp_file_path)
                        except OSError as cleanup_err:
                            print(
                                f"worker-comfyui - Warning: could not remove temp file {temp_file_path}: {cleanup_err}"
                            )
            except Exception as upload_error:
                error_msg = (
                    f"Error uploading {safe_filename} for job '{job_label}' to S3: {upload_error}"
                )
                print(f"worker-comfyui - {error_msg}")
                errors.append(error_msg)
        else:
            try:
                base64_payload = base64.b64encode(file_bytes).decode("utf-8")
                output_data.append(
                    {
                        "filename": safe_filename,
                        "type": "base64",
                        "media_kind": media_kind,
                        "data": base64_payload,
                    }
                )
                print(
                    f"worker-comfyui - Encoded {safe_filename} for job '{job_label}' as base64"
                )
            except Exception as encode_error:
                error_msg = (
                    f"Error encoding {safe_filename} for job '{job_label}' to base64: {encode_error}"
                )
                print(f"worker-comfyui - {error_msg}")
                errors.append(error_msg)

    def _collect_media(node_id, node_output, node_output_key, media_kind):
        media_entries = node_output.get(node_output_key, [])
        if not media_entries:
            return
        print(
            f"worker-comfyui - Node {node_id} contains {len(media_entries)} {media_kind}(s) for job '{job_label}'"
        )
        for media_info in media_entries:
            filename = media_info.get("filename")
            subfolder = media_info.get("subfolder", "")
            resource_type = media_info.get("type")

            if media_kind == "image" and resource_type == "temp":
                print(
                    f"worker-comfyui - Skipping temp image {filename} for job '{job_label}'"
                )
                continue

            if not filename:
                warn_msg = f"Skipping {media_kind} in node {node_id} due to missing filename"
                print(f"worker-comfyui - {warn_msg}")
                errors.append(warn_msg)
                continue

            file_bytes = fetch_output_file(filename, subfolder, resource_type)
            if file_bytes is None:
                errors.append(
                    f"Failed to fetch {media_kind} data for {filename} from /view endpoint."
                )
                continue

            _store_media_payload(filename, media_kind, file_bytes)

    try:
        ws_url = f"ws://{COMFY_HOST}/ws?clientId={client_id}"
        print(f"worker-comfyui - Connecting to websocket for job '{job_label}': {ws_url}")
        ws = websocket.WebSocket()
        ws.connect(ws_url, timeout=10)
        print(f"worker-comfyui - Websocket connected for job '{job_label}'")

        if input_images:
            upload_result = upload_images(input_images)
            if upload_result["status"] == "error":
                raise ValueError(
                    f"Job '{job_label}' failed to upload one or more input images: {upload_result['details']}"
                )

        queued_workflow = queue_workflow(workflow, client_id)
        prompt_id = queued_workflow.get("prompt_id")
        if not prompt_id:
            raise ValueError(
                f"Job '{job_label}': Missing 'prompt_id' in queue response: {queued_workflow}"
            )
        print(
            f"worker-comfyui - Queued workflow with ID {prompt_id} for job '{job_label}'"
        )

        print(
            f"worker-comfyui - Waiting for workflow execution ({prompt_id}) for job '{job_label}'..."
        )
        execution_done = False
        while True:
            try:
                out = ws.recv()
                if isinstance(out, str):
                    message = json.loads(out)
                    if message.get("type") == "status":
                        status_data = message.get("data", {}).get("status", {})
                        print(
                            f"worker-comfyui - Job '{job_label}' status update: {status_data.get('exec_info', {}).get('queue_remaining', 'N/A')} items remaining in queue"
                        )
                    elif message.get("type") == "executing":
                        data = message.get("data", {})
                        if (
                            data.get("node") is None
                            and data.get("prompt_id") == prompt_id
                        ):
                            print(
                                f"worker-comfyui - Execution finished for prompt {prompt_id} (job '{job_label}')"
                            )
                            execution_done = True
                            break
                    elif message.get("type") == "execution_error":
                        data = message.get("data", {})
                        if data.get("prompt_id") == prompt_id:
                            error_details = f"Node Type: {data.get('node_type')}, Node ID: {data.get('node_id')}, Message: {data.get('exception_message')}"
                            print(
                                f"worker-comfyui - Execution error for job '{job_label}': {error_details}"
                            )
                            errors.append(f"Workflow execution error: {error_details}")
                            break
                else:
                    continue
            except websocket.WebSocketTimeoutException:
                print(
                    f"worker-comfyui - Websocket receive timed out for job '{job_label}'. Still waiting..."
                )
                continue
            except websocket.WebSocketConnectionClosedException as closed_err:
                try:
                    ws = _attempt_websocket_reconnect(
                        ws_url,
                        WEBSOCKET_RECONNECT_ATTEMPTS,
                        WEBSOCKET_RECONNECT_DELAY_S,
                        closed_err,
                    )
                    print(
                        f"worker-comfyui - Resumed websocket after reconnect for job '{job_label}'"
                    )
                    continue
                except websocket.WebSocketConnectionClosedException as reconn_failed:
                    raise reconn_failed
            except json.JSONDecodeError:
                print(
                    f"worker-comfyui - Job '{job_label}' received invalid JSON message via websocket."
                )

        if not execution_done and not errors:
            raise ValueError(
                f"Job '{job_label}': Workflow monitoring loop exited without completion or error."
            )

        print(f"worker-comfyui - Fetching history for prompt {prompt_id} (job '{job_label}')...")
        history = get_history(prompt_id)

        if prompt_id not in history:
            raise ValueError(
                f"Job '{job_label}': Prompt ID {prompt_id} not found in history after execution."
            )

        prompt_history = history.get(prompt_id, {})
        outputs = prompt_history.get("outputs", {})

        if not outputs:
            warning_msg = (
                f"Job '{job_label}': No outputs found in history for prompt {prompt_id}."
            )
            print(f"worker-comfyui - {warning_msg}")
            errors.append(warning_msg)

        print(
            f"worker-comfyui - Processing {len(outputs)} output nodes for job '{job_label}'..."
        )
        for node_id, node_output in outputs.items():
            _collect_media(node_id, node_output, "images", "image")
            _collect_media(node_id, node_output, "videos", "video")

            other_keys = [
                k for k in node_output.keys() if k not in ("images", "videos")
            ]
            if other_keys:
                warn_msg = (
                    f"Node {node_id} produced unhandled output keys: {other_keys} for job '{job_label}'."
                )
                print(f"worker-comfyui - WARNING: {warn_msg}")
                errors.append(warn_msg)

    except websocket.WebSocketException as e:
        print(f"worker-comfyui - WebSocket Error in job '{job_label}': {e}")
        print(traceback.format_exc())
        errors.append(f"WebSocket communication error: {e}")
    except requests.RequestException as e:
        print(f"worker-comfyui - HTTP Request Error in job '{job_label}': {e}")
        print(traceback.format_exc())
        errors.append(f"HTTP communication error with ComfyUI: {e}")
    except ValueError as e:
        print(f"worker-comfyui - Value Error in job '{job_label}': {e}")
        print(traceback.format_exc())
        errors.append(str(e))
    except Exception as e:
        print(f"worker-comfyui - Unexpected Error in job '{job_label}': {e}")
        print(traceback.format_exc())
        errors.append(f"An unexpected error occurred: {e}")
    finally:
        if ws and ws.connected:
            print(f"worker-comfyui - Closing websocket connection for job '{job_label}'.")
            ws.close()

    job_result = {"job_label": job_label, "media": output_data}

    if errors and not output_data:
        job_result["status"] = "error"
        job_result["errors"] = errors
    elif errors:
        job_result["status"] = "completed_with_warnings"
        job_result["errors"] = errors
    else:
        job_result["status"] = "success"

    print(
        f"worker-comfyui - Job '{job_label}' completed with {len(output_data)} artifact(s)."
    )
    return {"result": job_result, "binary_outputs": binary_outputs}


def handler(job):
    """
    Handles a job using ComfyUI via websockets for status and media retrieval.

    Args:
        job (dict): A dictionary containing job details and input parameters.

    Returns:
        dict: Aggregated results for every batch, plus an optional ZIP artifact.
    """
    job_input = job["input"]
    job_id = job["id"]

    validated_data, error_message = validate_input(job_input)
    if error_message:
        return {"error": error_message}

    if not check_server(
        f"http://{COMFY_HOST}/",
        COMFY_API_AVAILABLE_MAX_RETRIES,
        COMFY_API_AVAILABLE_INTERVAL_MS,
    ):
        return {
            "error": f"ComfyUI server ({COMFY_HOST}) not reachable after multiple retries."
        }

    combined_results = []
    binary_outputs = []
    any_success = False

    for job_payload in validated_data["jobs"]:
        job_label = job_payload["job_label"]
        mode = (job_payload.get("mode") or validated_data.get("mode") or "").strip().lower()

        try:
            if job_payload.get("workflow") is not None:
                workflow = job_payload["workflow"]
                images = job_payload.get("images") or []
            elif mode == MODE_REF_VIDEO:
                workflow = build_ref_video_workflow(job_payload)
                images = []  # assets persisted to disk for this mode
            else:
                raise ValueError(
                    f"Job '{job_label}': unsupported or missing workflow/mode. "
                    "Provide 'workflow' or mode 'ref_video_lora'."
                )
        except Exception as build_err:
            combined_results.append(
                {"job_label": job_label, "status": "error", "errors": [str(build_err)]}
            )
            continue

        execution = execute_workflow_job(job_id, job_label, workflow, images)
        combined_results.append(execution["result"])
        binary_outputs.extend(execution["binary_outputs"])

        if execution["result"].get("status") in ("success", "completed_with_warnings"):
            any_success = True

    final_response = {"jobs": combined_results}

    if validated_data.get("zip_outputs", True):
        video_binary_outputs = [
            entry for entry in binary_outputs if entry.get("media_kind") == "video"
        ]
        zip_entry = create_zip_archive(
            video_binary_outputs, validated_data.get("zip_filename_prefix") or job_id
        )
        if zip_entry:
            final_response["zip_file"] = zip_entry

    if not any_success:
        final_response["error"] = (
            "All batch jobs failed. Check individual job errors for details."
        )

    return final_response


if __name__ == "__main__":
    print("worker-comfyui - Starting handler...")
    runpod.serverless.start({"handler": handler})
