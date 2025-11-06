import runpod
from runpod.serverless.utils import rp_upload
import json
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
import copy
import zipfile

# -------------------------------
# Config
# -------------------------------
COMFY_HOST = "127.0.0.1:8188"
COMFY_API_AVAILABLE_INTERVAL_MS = 50
COMFY_API_AVAILABLE_MAX_RETRIES = 500

WEBSOCKET_RECONNECT_ATTEMPTS = int(os.environ.get("WEBSOCKET_RECONNECT_ATTEMPTS", 5))
WEBSOCKET_RECONNECT_DELAY_S = int(os.environ.get("WEBSOCKET_RECONNECT_DELAY_S", 3))

if os.environ.get("WEBSOCKET_TRACE", "false").lower() == "true":
    websocket.enableTrace(True)

# -------------------------------
# Helpers
# -------------------------------
def _comfy_server_status():
    try:
        resp = requests.get(f"http://{COMFY_HOST}/", timeout=5)
        return {"reachable": resp.status_code == 200, "status_code": resp.status_code}
    except Exception as exc:
        return {"reachable": False, "error": str(exc)}

def _attempt_websocket_reconnect(ws_url, max_attempts, delay_s, initial_error):
    print(f"worker-comfyui - Websocket closed: {initial_error}. Reconnecting...")
    last_err = initial_error
    for attempt in range(max_attempts):
        srv = _comfy_server_status()
        if not srv["reachable"]:
            print("worker-comfyui - ComfyUI HTTP unreachable during reconnect.")
            raise websocket.WebSocketConnectionClosedException(
                "ComfyUI HTTP unreachable during websocket reconnect"
            )
        print(f"worker-comfyui - Reconnect attempt {attempt+1}/{max_attempts} (HTTP {srv.get('status_code')})")
        try:
            new_ws = websocket.WebSocket()
            new_ws.connect(ws_url, timeout=10)
            print("worker-comfyui - Websocket reconnected.")
            return new_ws
        except (websocket.WebSocketException, ConnectionRefusedError, socket.timeout, OSError) as e:
            last_err = e
            if attempt < max_attempts - 1:
                time.sleep(delay_s)
    raise websocket.WebSocketConnectionClosedException(
        f"Failed to reconnect websocket. Last error: {last_err}"
    )

def validate_input(job_input):
    if job_input is None:
        return None, "Please provide input"
    if isinstance(job_input, str):
        try:
            job_input = json.loads(job_input)
        except json.JSONDecodeError:
            return None, "Invalid JSON format in input"

    workflow = job_input.get("workflow")
    if workflow is None:
        return None, "Missing 'workflow' parameter"

    images = job_input.get("images")
    if images is not None:
        if not isinstance(images, list) or not all("name" in i and "image" in i for i in images):
            return None, "'images' must be a list of objects with 'name' and 'image' keys"

    # optional overrides for batch injection
    image_node_id = str(job_input.get("image_node_id", "97"))
    image_input_key = str(job_input.get("image_input_key", "image"))
    batch_name = str(job_input.get("batch_name", f"batch_{int(time.time())}"))

    return {
        "workflow": workflow,
        "images": images,
        "image_node_id": image_node_id,
        "image_input_key": image_input_key,
        "batch_name": batch_name
    }, None

def check_server(url, retries=500, delay=50):
    print(f"worker-comfyui - Checking API server at {url}...")
    for _ in range(retries):
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                print("worker-comfyui - API is reachable")
                return True
        except requests.RequestException:
            pass
        time.sleep(delay / 1000)
    print("worker-comfyui - API not reachable after retries.")
    return False

def upload_images(images):
    if not images:
        return {"status": "success", "message": "No images to upload", "details": []}
    responses, errs = [], []
    print(f"worker-comfyui - Uploading {len(images)} image(s)...")
    for image in images:
        try:
            name = image["name"]
            image_data_uri = image["image"]
            if "," in image_data_uri:
                base64_data = image_data_uri.split(",", 1)[1]
            else:
                base64_data = image_data_uri
            blob = base64.b64decode(base64_data)
            files = {
                "image": (name, BytesIO(blob), "image/png"),
                "overwrite": (None, "true"),
            }
            resp = requests.post(f"http://{COMFY_HOST}/upload/image", files=files, timeout=60)
            resp.raise_for_status()
            responses.append(f"Uploaded {name}")
        except Exception as e:
            msg = f"Upload failed for {image.get('name','unknown')}: {e}"
            print(f"worker-comfyui - {msg}")
            errs.append(msg)
    if errs:
        return {"status": "error", "message": "Some images failed to upload", "details": errs}
    return {"status": "success", "message": "All images uploaded", "details": responses}

def queue_workflow(workflow, client_id):
    payload = {"prompt": workflow, "client_id": client_id}
    headers = {"Content-Type": "application/json"}
    r = requests.post(f"http://{COMFY_HOST}/prompt", data=json.dumps(payload).encode("utf-8"), headers=headers, timeout=60)
    if r.status_code == 400:
        # Try to bubble up helpful error
        try:
            err = r.json()
        except Exception:
            err = r.text
        raise ValueError(f"Workflow validation failed: {err}")
    r.raise_for_status()
    return r.json()

def get_history(prompt_id):
    r = requests.get(f"http://{COMFY_HOST}/history/{prompt_id}", timeout=60)
    r.raise_for_status()
    return r.json()

def get_image_data(filename, subfolder, image_type):
    data = {"filename": filename, "subfolder": subfolder, "type": image_type}
    try:
        r = requests.get(f"http://{COMFY_HOST}/view?{urllib.parse.urlencode(data)}", timeout=120)
        r.raise_for_status()
        return r.content
    except requests.RequestException as e:
        print(f"worker-comfyui - fetch /view failed for {filename}: {e}")
        return None

def _collect_outputs(job_id, prompt_id):
    """Return list of {'filename','bytes'} for non-temp images."""
    out_files = []
    hist = get_history(prompt_id)
    prompt_hist = hist.get(prompt_id, {})
    outputs = prompt_hist.get("outputs", {})
    for node_id, node_output in outputs.items():
        if "images" not in node_output:
            continue
        for image_info in node_output["images"]:
            if image_info.get("type") == "temp":
                continue
            fname = image_info.get("filename")
            subf = image_info.get("subfolder", "")
            if not fname:
                continue
            raw = get_image_data(fname, subf, image_info.get("type"))
            if raw:
                out_files.append({"filename": fname, "bytes": raw})
    return out_files

def _save_and_optionally_upload(job_id, files):
    """
    Save outputs and return:
      - images: [{filename, type: base64|s3_url, data}]
      - zip_url or zip_base64
    """
    results = {"images": []}
    use_s3 = bool(os.environ.get("BUCKET_ENDPOINT_URL"))
    tmpdir = tempfile.mkdtemp(prefix=f"{job_id}_")
    zip_path = os.path.join(tmpdir, "outputs.zip")

    # write files locally for zipping; also upload/encode individually
    for f in files:
        local_path = os.path.join(tmpdir, f["filename"])
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as fh:
            fh.write(f["bytes"])

        if use_s3:
            try:
                url = rp_upload.upload_image(job_id, local_path)
                results["images"].append({"filename": f["filename"], "type": "s3_url", "data": url})
            except Exception as e:
                # fallback to base64
                b64 = base64.b64encode(f["bytes"]).decode("utf-8")
                results["images"].append({"filename": f["filename"], "type": "base64", "data": b64})
        else:
            b64 = base64.b64encode(f["bytes"]).decode("utf-8")
            results["images"].append({"filename": f["filename"], "type": "base64", "data": b64})

    # build ZIP
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            local_path = os.path.join(tmpdir, f["filename"])
            arcname = f["filename"]
            zf.write(local_path, arcname=arcname)

    if use_s3:
        # try generic file upload if available; otherwise encode base64
        try:
            # rp_upload.upload_file may not exist in older utils. Try it; fallback to image.
            if hasattr(rp_upload, "upload_file"):
                zip_url = rp_upload.upload_file(job_id, zip_path)
            else:
                zip_url = rp_upload.upload_image(job_id, zip_path)  # works but labeled as image
            results["zip_url"] = zip_url
        except Exception as e:
            with open(zip_path, "rb") as fh:
                results["zip_base64"] = base64.b64encode(fh.read()).decode("utf-8")
    else:
        with open(zip_path, "rb") as fh:
            results["zip_base64"] = base64.b64encode(fh.read()).decode("utf-8")

    return results

def _run_single_workflow_for_image(base_workflow, image_node_id, image_input_key, image_filename):
    """
    Return (prompt_id, ws) after queuing; waits for execution to finish and closes ws.
    """
    # deep copy and inject image filename
    wf = copy.deepcopy(base_workflow)
    if image_node_id not in wf:
        raise ValueError(f"Workflow missing image node '{image_node_id}'.")
    if "inputs" not in wf[image_node_id]:
        wf[image_node_id]["inputs"] = {}
    wf[image_node_id]["inputs"][image_input_key] = image_filename

    client_id = str(uuid.uuid4())
    ws_url = f"ws://{COMFY_HOST}/ws?clientId={client_id}"
    ws = websocket.WebSocket()
    ws.connect(ws_url, timeout=10)

    try:
        queued = queue_workflow(wf, client_id)
        prompt_id = queued.get("prompt_id")
        if not prompt_id:
            raise ValueError(f"Missing 'prompt_id' in queue response: {queued}")

        # wait until executing finished (executing event with node=None)
        while True:
            out = ws.recv()
            if not isinstance(out, str):
                continue
            msg = json.loads(out)
            if msg.get("type") == "executing":
                data = msg.get("data", {})
                if data.get("prompt_id") == prompt_id and data.get("node") is None:
                    break
            elif msg.get("type") == "execution_error":
                data = msg.get("data", {})
                if data.get("prompt_id") == prompt_id:
                    info = f"Node Type: {data.get('node_type')}, Node ID: {data.get('node_id')}, Message: {data.get('exception_message')}"
                    raise ValueError(f"Workflow execution error: {info}")
    finally:
        if ws and ws.connected:
            ws.close()

    return prompt_id

# -------------------------------
# Main handler (batch-aware)
# -------------------------------
def handler(job):
    job_input = job["input"]
    job_id = job["id"]

    validated, err = validate_input(job_input)
    if err:
        return {"error": err}

    workflow = validated["workflow"]
    images = validated["images"] or []  # may be empty/single-image flow
    image_node_id = validated["image_node_id"]
    image_input_key = validated["image_input_key"]
    batch_name = validated["batch_name"]

    # Ensure API up
    if not check_server(f"http://{COMFY_HOST}/", COMFY_API_AVAILABLE_MAX_RETRIES, COMFY_API_AVAILABLE_INTERVAL_MS):
        return {"error": f"ComfyUI server ({COMFY_HOST}) not reachable after retries."}

    # Upload all images first (if any)
    if images:
        up = upload_images(images)
        if up["status"] == "error":
            return {"error": "Failed to upload one or more input images", "details": up["details"]}

    # If no images provided, run once with the workflow "as is" (backward compatible)
    prompts_to_run = []
    if not images:
        prompts_to_run = [{"label": "single", "filename": None}]
    else:
        prompts_to_run = [{"label": os.path.splitext(img["name"])[0], "filename": img["name"]} for img in images]

    all_files = []   # list of {'filename','bytes'} across the whole batch
    per_image_results = []  # for metadata mapping

    try:
        for item in prompts_to_run:
            try:
                prompt_id = _run_single_workflow_for_image(
                    workflow,
                    image_node_id=image_node_id,
                    image_input_key=image_input_key,
                    image_filename=item["filename"] if item["filename"] else workflow.get(image_node_id, {}).get("inputs", {}).get(image_input_key)
                )
                files = _collect_outputs(job_id, prompt_id)
                # Prefix filenames to keep batches tidy
                for f in files:
                    label = item["label"] or "single"
                    # Ensure unique/organized names in the zip
                    base = f["filename"]
                    f["filename"] = f"{label}/{base}"
                all_files.extend(files)
                per_image_results.append({"image": item["filename"], "outputs": [f["filename"] for f in files]})
            except Exception as e:
                # continue other images but record error
                per_image_results.append({"image": item["filename"], "error": str(e)})
                continue

        if not all_files:
            return {"error": "No outputs were produced for the supplied workflow/images.", "details": per_image_results}

        packaged = _save_and_optionally_upload(job_id, all_files)

        # attach some metadata
        packaged["batch"] = {
            "name": batch_name,
            "count_inputs": len(images) if images else 1,
            "count_outputs": len(all_files),
            "mapping": per_image_results
        }
        print(f"worker-comfyui - Batch done. Inputs: {len(images) if images else 1}, Outputs: {len(all_files)}")
        return packaged

    except websocket.WebSocketException as e:
        print(traceback.format_exc())
        return {"error": f"WebSocket communication error: {e}"}
    except requests.RequestException as e:
        print(traceback.format_exc())
        return {"error": f"HTTP communication error with ComfyUI: {e}"}
    except ValueError as e:
        print(traceback.format_exc())
        return {"error": str(e)}
    except Exception as e:
        print(traceback.format_exc())
        return {"error": f"An unexpected error occurred: {e}"}

if __name__ == "__main__":
    print("worker-comfyui - Starting handler (batch-enabled)...")
    runpod.serverless.start({"handler": handler})
