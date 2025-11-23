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
import copy

# Path to the Whole Body Replacer workflow
WHOLEBODY_WORKFLOW_PATH = "test_resources/workflows/wholebodyReplacer.json"

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

    if jobs_payload is not None:
        if not isinstance(jobs_payload, list) or len(jobs_payload) == 0:
            return None, "'jobs' must be a non-empty list of workflow definitions"

        for idx, job_definition in enumerate(jobs_payload, start=1):
            if not isinstance(job_definition, dict):
                return None, f"'jobs[{idx}]' must be an object"

            workflow = job_definition.get("workflow")
            mode = job_definition.get("mode")
            if workflow is None and mode != "ref_video_lora":
                return None, f"'workflow' missing in jobs[{idx}]"

            images, image_error = _validate_images_field(
                job_definition.get("images"), f"jobs[{idx}]"
            )
            if image_error:
                return None, image_error

            job_label = (
                job_definition.get("job_label")
                or job_definition.get("label")
                or job_definition.get("id")
                or f"job-{idx}"
            )

            validated_jobs.append(
                    "job_label": job_label,
                    "workflow": workflow,
                    "images": images,
                    # Pass through new fields for wholebody workflow
                    "mode": job_definition.get("mode"),
                    "prompt": job_definition.get("prompt"),
                    "neg_prompt": job_definition.get("neg_prompt"),
                    "character_image": job_definition.get("character_image"),
                    "character_image_name": job_definition.get("character_image_name"),
                    "reference_video": job_definition.get("reference_video"),
                    "reference_video_name": job_definition.get("reference_video_name"),
                    "lora_name": job_definition.get("lora_name"),
                    "use_face_crop": job_definition.get("use_face_crop"),
                    "width": job_definition.get("width"),
                    "height": job_definition.get("height"),
                    "video_length": job_definition.get("video_length"),
                }
            )
    else:
        workflow = job_input.get("workflow")
        mode = job_input.get("mode")
        
        if workflow is None and mode != "ref_video_lora":
            return None, "Missing 'workflow' parameter"

        images, image_error = _validate_images_field(
            job_input.get("images"), "input"
        )
        if image_error:
            return None, image_error

        validated_jobs.append(
            {
                "job_label": job_input.get("job_label") or "job-1",
                "workflow": workflow,
                "images": images,
                # Pass through new fields for wholebody workflow
                "mode": job_input.get("mode"),
                "prompt": job_input.get("prompt"),
                "neg_prompt": job_input.get("neg_prompt"),
                "character_image": job_input.get("character_image"),
                "character_image_name": job_input.get("character_image_name"),
                "reference_video": job_input.get("reference_video"),
                "reference_video_name": job_input.get("reference_video_name"),
                "lora_name": job_input.get("lora_name"),
                "use_face_crop": job_input.get("use_face_crop"),
                "width": job_input.get("width"),
                "height": job_input.get("height"),
                "video_length": job_input.get("video_length"),
            }
        )

    return (
        {
            "jobs": validated_jobs,
            "zip_outputs": bool(job_input.get("zip_outputs", True)),
            "zip_filename_prefix": job_input.get("zip_filename_prefix")
            or job_input.get("zip_prefix"),
        },
        None,
    )


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


def upload_media(media_items):
    """
    Upload a list of base64 encoded media (images/videos) to the ComfyUI server.

    Args:
        media_items (list): A list of dictionaries, each containing:
            - 'name': filename
            - 'image' or 'video': base64 encoded string
            - 'type': (optional) 'image' or 'video', defaults to image logic

    Returns:
        dict: A dictionary indicating success or error.
    """
    if not media_items:
        return {"status": "success", "message": "No media to upload", "details": []}

    responses = []
    upload_errors = []

    print(f"worker-comfyui - Uploading {len(media_items)} media item(s)...")

    for item in media_items:
        try:
            name = item.get("name")
            # Support both 'image' and 'video' keys, or generic 'data'
            data_uri = item.get("image") or item.get("video") or item.get("data")
            
            if not data_uri:
                continue

            # --- Strip Data URI prefix if present ---
            if "," in data_uri:
                base64_data = data_uri.split(",", 1)[1]
            else:
                base64_data = data_uri
            # --- End strip ---

            blob = base64.b64decode(base64_data)
            
            # Determine mime type based on extension or default
            ext = os.path.splitext(name)[1].lower()
            if ext in ['.mp4', '.webm', '.mov', '.avi']:
                content_type = "video/mp4" # ComfyUI handles most video uploads via same endpoint
                upload_type = "video"
            else:
                content_type = "image/png"
                upload_type = "image"

            # Prepare the form data
            files = {
                "image": (name, BytesIO(blob), content_type),
                "overwrite": (None, "true"),
            }

            # POST request to upload
            # ComfyUI uses /upload/image for both images and videos usually
            response = requests.post(
                f"http://{COMFY_HOST}/upload/image", files=files, timeout=60
            )
            response.raise_for_status()

            responses.append(f"Successfully uploaded {name}")
            print(f"worker-comfyui - Successfully uploaded {name}")

        except Exception as e:
            error_msg = f"Error uploading {item.get('name', 'unknown')}: {e}"
            print(f"worker-comfyui - {error_msg}")
            upload_errors.append(error_msg)

    if upload_errors:
        return {
            "status": "error",
            "message": "Some media failed to upload",
            "details": upload_errors,
        }

    return {
        "status": "success",
        "message": "All media uploaded successfully",
        "details": responses,
    }


def upload_images(images):
    """Backwards compatibility wrapper for upload_media"""
    return upload_media(images)


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


def load_wholebody_workflow(job_data):
    """
    Load and populate the Whole Body Replacer workflow.
    """
    try:
        with open(WHOLEBODY_WORKFLOW_PATH, "r") as f:
            workflow = json.load(f)
    except FileNotFoundError:
        raise ValueError(f"Workflow file not found at {WHOLEBODY_WORKFLOW_PATH}")

    # Helper to set widget values
    def _set_widget(node_id, widget_index, value):
        if str(node_id) in workflow:
            # Some nodes use 'widgets_values'
            if "widgets_values" in workflow[str(node_id)]:
                # Ensure list is long enough
                while len(workflow[str(node_id)]["widgets_values"]) <= widget_index:
                    workflow[str(node_id)]["widgets_values"].append(None)
                workflow[str(node_id)]["widgets_values"][widget_index] = value
            # Some might use inputs (but this JSON format seems to use widgets_values for user inputs)
            elif "inputs" in workflow[str(node_id)]:
                 # This depends on the node type, but for the identified nodes:
                 pass

    # 1. Character Image (Node 311)
    char_img_name = job_data.get("character_image_name")
    if char_img_name:
        _set_widget(311, 0, char_img_name)

    # 2. Reference Video (Node 417)
    ref_vid_name = job_data.get("reference_video_name")
    if ref_vid_name:
        # Widget index 0 is 'video'
        _set_widget(417, 0, ref_vid_name)
        # Also update the preview params if present (optional but good for consistency)
        if "widgets_values" in workflow["417"] and isinstance(workflow["417"]["widgets_values"], dict):
             workflow["417"]["widgets_values"]["video"] = ref_vid_name

    # 3. Prompt (Node 227)
    prompt = job_data.get("prompt")
    if prompt:
        _set_widget(227, 0, prompt)

    # 4. Negative Prompt (Node 228)
    neg_prompt = job_data.get("neg_prompt")
    if neg_prompt:
        _set_widget(228, 0, neg_prompt)

    # 5. LoRA (Node 315)
    lora_name = job_data.get("lora_name")
    if lora_name:
        _set_widget(315, 0, lora_name)
        # Ensure node is enabled (mode 0)
        workflow["315"]["mode"] = 0
    else:
        # Bypass/Disable LoRA node if no LoRA provided
        # Mode 4 is typically 'Bypass' or 'Never'
        workflow["315"]["mode"] = 4

    # 6. Face Crop (Node 432)
    use_face_crop = job_data.get("use_face_crop")
    if use_face_crop:
        workflow["432"]["mode"] = 0 # Enable
    else:
        workflow["432"]["mode"] = 4 # Bypass

    # 7. Dimensions
    width = job_data.get("width")
    if width:
        _set_widget(333, 0, width) # Set_width uses PrimitiveInt inputs? No, Node 333 is SetNode inputting from 330 (Video Width)
        # Wait, Node 333 is SetNode. The value comes from Node 330 (PrimitiveInt).
        # Node 330 widgets_values[0] is the value.
        _set_widget(330, 0, width)
    
    height = job_data.get("height")
    if height:
        # Node 332 is SetNode, input from Node 331 (Video Height - PrimitiveInt)
        _set_widget(331, 0, height)

    length = job_data.get("video_length")
    if length:
        # Node 383 is Video Length (PrimitiveInt)
        _set_widget(383, 0, length)

    return workflow


def execute_workflow_job(runpod_job_id, job_label, workflow, input_images, job_data=None):
    """
    Execute a single ComfyUI workflow for an individual batch and collect outputs.
    """
    print(f"worker-comfyui - Starting workflow job '{job_label}'")
    
    # Handle Whole Body Replacer mode
    if job_data and job_data.get("mode") == "ref_video_lora":
        print(f"worker-comfyui - Detected Whole Body Replacer mode for job '{job_label}'")
        try:
            workflow = load_wholebody_workflow(job_data)
            
            # Prepare uploads for this mode
            uploads = []
            if job_data.get("character_image"):
                uploads.append({
                    "name": job_data.get("character_image_name"),
                    "image": job_data.get("character_image")
                })
            if job_data.get("reference_video"):
                uploads.append({
                    "name": job_data.get("reference_video_name"),
                    "video": job_data.get("reference_video")
                })
            
            # Upload media
            upload_result = upload_media(uploads)
            if upload_result["status"] == "error":
                raise ValueError(f"Failed to upload media: {upload_result['details']}")
                
        except Exception as e:
            print(f"worker-comfyui - Error preparing wholebody workflow: {e}")
            return {
                "result": {"job_label": job_label, "status": "error", "errors": [str(e)]},
                "binary_outputs": []
            }
            
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
        workflow = job_payload["workflow"]
        images = job_payload.get("images") or []

        # Pass the full job payload to execute_workflow_job
        execution = execute_workflow_job(job_id, job_label, workflow, images, job_data=job_payload)
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
