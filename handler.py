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

# Time to wait between API check attempts in milliseconds
COMFY_API_AVAILABLE_INTERVAL_MS = 50
# Maximum number of API check attempts
COMFY_API_AVAILABLE_MAX_RETRIES = 500
# Websocket reconnection behaviour
WEBSOCKET_RECONNECT_ATTEMPTS = int(os.environ.get('WEBSOCKET_RECONNECT_ATTEMPTS', 5))
WEBSOCKET_RECONNECT_DELAY_S = int(os.environ.get('WEBSOCKET_RECONNECT_DELAY_S', 3))

# Extra verbose websocket trace logs
if os.environ.get('WEBSOCKET_TRACE', 'false').lower() == 'true':
	websocket.enableTrace(True)

# Host where ComfyUI is running
COMFY_HOST = '127.0.0.1:8188'
# Enforce a clean state after each job is done
REFRESH_WORKER = os.environ.get('REFRESH_WORKER', 'false').lower() == 'true'

# ---------------------------------------------------------------------------
# Helper: quick reachability probe of ComfyUI HTTP endpoint (port 8188)
# ---------------------------------------------------------------------------

def _comfy_server_status():
	"""Return a dictionary with basic reachability info for the ComfyUI HTTP server."""
	try:
		resp = requests.get(f'http://{COMFY_HOST}/', timeout=5)
		return {
			'reachable': resp.status_code == 200,
			'status_code': resp.status_code,
		}
	except Exception as exc:
		return {'reachable': False, 'error': str(exc)}

def _attempt_websocket_reconnect(ws_url, max_attempts, delay_s, initial_error):
	"""
	Attempts to reconnect to the WebSocket server after a disconnect.
	"""
	print(
		f'worker-comfyui - Websocket connection closed unexpectedly: {initial_error}. Attempting to reconnect...'
	)
	last_reconnect_error = initial_error
	for attempt in range(max_attempts):
		srv_status = _comfy_server_status()
		if not srv_status['reachable']:
			print(
				f"worker-comfyui - ComfyUI HTTP unreachable – aborting websocket reconnect: {srv_status.get('error', 'status '+str(srv_status.get('status_code')))}"
			)
			raise websocket.WebSocketConnectionClosedException(
				'ComfyUI HTTP unreachable during websocket reconnect'
			)

		print(
			f"worker-comfyui - Reconnect attempt {attempt + 1}/{max_attempts}... (ComfyUI HTTP reachable, status {srv_status.get('status_code')})"
		)
		try:
			new_ws = websocket.WebSocket()
			new_ws.connect(ws_url, timeout=10)
			print(f'worker-comfyui - Websocket reconnected successfully.')
			return new_ws
		except (
			websocket.WebSocketException,
			ConnectionRefusedError,
			socket.timeout,
			OSError,
		) as reconn_err:
			last_reconnect_error = reconn_err
			print(
				f'worker-comfyui - Reconnect attempt {attempt + 1} failed: {reconn_err}'
			)
			if attempt < max_attempts - 1:
				print(
					f'worker-comfyui - Waiting {delay_s} seconds before next attempt...'
				)
				time.sleep(delay_s)
			else:
				print(f'worker-comfyui - Max reconnection attempts reached.')

	print('worker-comfyui - Failed to reconnect websocket after connection closed.')
	raise websocket.WebSocketConnectionClosedException(
		f'Connection closed and failed to reconnect. Last error: {last_reconnect_error}'
	)

def validate_input(job_input):
	"""
	Validates the input for the handler function.
	Supports both single image and batch processing.
	"""
	if job_input is None:
		return None, 'Please provide input'

	if isinstance(job_input, str):
		try:
			job_input = json.loads(job_input)
		except json.JSONDecodeError:
			return None, 'Invalid JSON format in input'

	# Check for batch_id (batch processing)
	batch_id = job_input.get('batch_id')
	
	# Validate 'workflow' in input
	workflow = job_input.get('workflow')
	if workflow is None:
		return None, "Missing 'workflow' parameter"

	# Validate 'images' in input (required for batch processing)
	images = job_input.get('images')
	if images is None:
		return None, "Missing 'images' parameter"
	
	if not isinstance(images, list) or len(images) == 0:
		return None, "'images' must be a non-empty list"
	
	# Validate each image has required fields
	for idx, image in enumerate(images):
		if not isinstance(image, dict):
			return None, f"Image at index {idx} must be an object"
		if 'name' not in image or 'image' not in image:
			return None, f"Image at index {idx} must have 'name' and 'image' keys"

	# Return validated data
	return {
		'workflow': workflow,
		'images': images,
		'batch_id': batch_id
	}, None

def check_server(url, retries=500, delay=50):
	"""Check if a server is reachable via HTTP GET request"""
	print(f'worker-comfyui - Checking API server at {url}...')
	for i in range(retries):
		try:
			response = requests.get(url, timeout=5)
			if response.status_code == 200:
				print(f'worker-comfyui - API is reachable')
				return True
		except requests.Timeout:
			pass
		except requests.RequestException as e:
			pass

		time.sleep(delay / 1000)

	print(
		f'worker-comfyui - Failed to connect to server at {url} after {retries} attempts.'
	)
	return False

def upload_images(images):
	"""Upload a list of base64 encoded images to the ComfyUI server"""
	if not images:
		return {'status': 'success', 'message': 'No images to upload', 'details': []}

	responses = []
	upload_errors = []

	print(f'worker-comfyui - Uploading {len(images)} image(s)...')

	for image in images:
		try:
			name = image['name']
			image_data_uri = image['image']

			# Strip Data URI prefix if present
			if ',' in image_data_uri:
				base64_data = image_data_uri.split(',', 1)[1]
			else:
				base64_data = image_data_uri

			blob = base64.b64decode(base64_data)

			# Prepare the form data
			files = {
				'image': (name, BytesIO(blob), 'image/png'),
				'overwrite': (None, 'true'),
			}

			# POST request to upload the image
			response = requests.post(
				f'http://{COMFY_HOST}/upload/image', files=files, timeout=30
			)
			response.raise_for_status()

			responses.append(f'Successfully uploaded {name}')
			print(f'worker-comfyui - Successfully uploaded {name}')

		except base64.binascii.Error as e:
			error_msg = f"Error decoding base64 for {image.get('name', 'unknown')}: {e}"
			print(f'worker-comfyui - {error_msg}')
			upload_errors.append(error_msg)
		except requests.Timeout:
			error_msg = f"Timeout uploading {image.get('name', 'unknown')}"
			print(f'worker-comfyui - {error_msg}')
			upload_errors.append(error_msg)
		except requests.RequestException as e:
			error_msg = f"Error uploading {image.get('name', 'unknown')}: {e}"
			print(f'worker-comfyui - {error_msg}')
			upload_errors.append(error_msg)
		except Exception as e:
			error_msg = (
				f"Unexpected error uploading {image.get('name', 'unknown')}: {e}"
			)
			print(f'worker-comfyui - {error_msg}')
			upload_errors.append(error_msg)

	if upload_errors:
		print(f'worker-comfyui - image(s) upload finished with errors')
		return {
			'status': 'error',
			'message': 'Some images failed to upload',
			'details': upload_errors,
		}

	print(f'worker-comfyui - image(s) upload complete')
	return {
		'status': 'success',
		'message': 'All images uploaded successfully',
		'details': responses,
	}

def queue_workflow(workflow, client_id):
	"""Queue a workflow to be processed by ComfyUI"""
	payload = {'prompt': workflow, 'client_id': client_id}
	data = json.dumps(payload).encode('utf-8')

	headers = {'Content-Type': 'application/json'}
	response = requests.post(
		f'http://{COMFY_HOST}/prompt', data=data, headers=headers, timeout=30
	)

	if response.status_code == 400:
		print(f'worker-comfyui - ComfyUI returned 400. Response body: {response.text}')
		try:
			error_data = response.json()
			print(f'worker-comfyui - Parsed error data: {error_data}')
			raise ValueError(f"Workflow validation failed: {response.text}")
		except (json.JSONDecodeError, KeyError) as e:
			raise ValueError(
				f'ComfyUI validation failed (could not parse error response): {response.text}'
			)

	response.raise_for_status()
	return response.json()

def get_history(prompt_id):
	"""Retrieve the history of a given prompt using its ID"""
	response = requests.get(f'http://{COMFY_HOST}/history/{prompt_id}', timeout=30)
	response.raise_for_status()
	return response.json()

def get_image_data(filename, subfolder, image_type):
	"""Fetch image bytes from the ComfyUI /view endpoint."""
	print(
		f'worker-comfyui - Fetching image data: type={image_type}, subfolder={subfolder}, filename={filename}'
	)
	data = {'filename': filename, 'subfolder': subfolder, 'type': image_type}
	url_values = urllib.parse.urlencode(data)
	try:
		response = requests.get(f'http://{COMFY_HOST}/view?{url_values}', timeout=60)
		response.raise_for_status()
		print(f'worker-comfyui - Successfully fetched image data for {filename}')
		return response.content
	except requests.Timeout:
		print(f'worker-comfyui - Timeout fetching image data for {filename}')
		return None
	except requests.RequestException as e:
		print(f'worker-comfyui - Error fetching image data for {filename}: {e}')
		return None
	except Exception as e:
		print(
			f'worker-comfyui - Unexpected error fetching image data for {filename}: {e}'
		)
		return None

def process_single_image(workflow, image_data, job_id, client_id, ws):
	"""
	Process a single image through the workflow.
	Returns the output video data or None if failed.
	"""
	try:
		# Upload the image
		upload_result = upload_images([image_data])
		if upload_result['status'] == 'error':
			return {
				'error': f"Failed to upload image {image_data.get('name')}",
				'details': upload_result['details']
			}

		# Queue the workflow
		try:
			queued_workflow = queue_workflow(workflow, client_id)
			prompt_id = queued_workflow.get('prompt_id')
			if not prompt_id:
				raise ValueError(
					f"Missing 'prompt_id' in queue response: {queued_workflow}"
				)
			print(f'worker-comfyui - Queued workflow with ID: {prompt_id}')
		except Exception as e:
			print(f'worker-comfyui - Error queuing workflow: {e}')
			return {'error': f'Error queuing workflow: {e}'}

		# Wait for execution completion via WebSocket
		print(f'worker-comfyui - Waiting for workflow execution ({prompt_id})...')
		execution_done = False
		execution_error = None
		
		while True:
			try:
				out = ws.recv()
				if isinstance(out, str):
					message = json.loads(out)
					if message.get('type') == 'status':
						status_data = message.get('data', {}).get('status', {})
						print(
							f"worker-comfyui - Status update: {status_data.get('exec_info', {}).get('queue_remaining', 'N/A')} items remaining in queue"
						)
					elif message.get('type') == 'executing':
						data = message.get('data', {})
						if (
							data.get('node') is None
							and data.get('prompt_id') == prompt_id
						):
							print(
								f'worker-comfyui - Execution finished for prompt {prompt_id}'
							)
							execution_done = True
							break
					elif message.get('type') == 'execution_error':
						data = message.get('data', {})
						if data.get('prompt_id') == prompt_id:
							error_details = f"Node Type: {data.get('node_type')}, Node ID: {data.get('node_id')}, Message: {data.get('exception_message')}"
							print(
								f'worker-comfyui - Execution error received: {error_details}'
							)
							execution_error = f'Workflow execution error: {error_details}'
							break
				else:
					continue
			except websocket.WebSocketTimeoutException:
				print(f'worker-comfyui - Websocket receive timed out. Still waiting...')
				continue
			except websocket.WebSocketConnectionClosedException as closed_err:
				# Websocket closed, but we'll let the caller handle reconnection
				raise closed_err
			except json.JSONDecodeError:
				print(f'worker-comfyui - Received invalid JSON message via websocket.')

		if execution_error:
			return {'error': execution_error}

		if not execution_done:
			return {'error': 'Workflow monitoring loop exited without confirmation of completion'}

		# Fetch history
		print(f'worker-comfyui - Fetching history for prompt {prompt_id}...')
		history = get_history(prompt_id)

		if prompt_id not in history:
			return {'error': f'Prompt ID {prompt_id} not found in history after execution'}

		prompt_history = history.get(prompt_id, {})
		outputs = prompt_history.get('outputs', {})

		if not outputs:
			return {'error': f'No outputs found in history for prompt {prompt_id}'}

		# Process outputs and get video data
		print(f'worker-comfyui - Processing {len(outputs)} output nodes...')
		for node_id, node_output in outputs.items():
			if 'images' in node_output:
				print(
					f'worker-comfyui - Node {node_id} contains {len(node_output["images"])} image(s)'
				)
				for image_info in node_output['images']:
					filename = image_info.get('filename')
					subfolder = image_info.get('subfolder', '')
					img_type = image_info.get('type')

					# Skip temp images
					if img_type == 'temp':
						print(
							f"worker-comfyui - Skipping image {filename} because type is 'temp'"
						)
						continue

					if not filename:
						continue

					image_bytes = get_image_data(filename, subfolder, img_type)

					if image_bytes:
						# Return the video data
						return {
							'filename': filename,
							'data': image_bytes,
							'success': True
						}

		return {'error': 'No valid output video found'}

	except Exception as e:
		print(f'worker-comfyui - Error processing single image: {e}')
		print(traceback.format_exc())
		return {'error': f'Error processing image: {e}'}

def handler(job):
	"""
	Handles a batch job - processes multiple images and returns videos in a zip file.
	"""
	job_input = job['input']
	job_id = job['id']

	# Validate input
	validated_data, error_message = validate_input(job_input)
	if error_message:
		return {'error': error_message}

	# Extract validated data
	workflow = validated_data['workflow']
	input_images = validated_data['images']
	batch_id = validated_data.get('batch_id', 'batch')

	print(f'worker-comfyui - Starting batch processing for {len(input_images)} images')
	print(f'worker-comfyui - Batch ID: {batch_id}')

	# Make sure ComfyUI is available
	if not check_server(
		f'http://{COMFY_HOST}/',
		COMFY_API_AVAILABLE_MAX_RETRIES,
		COMFY_API_AVAILABLE_INTERVAL_MS,
	):
		return {
			'error': f'ComfyUI server ({COMFY_HOST}) not reachable after multiple retries.'
		}

	ws = None
	client_id = str(uuid.uuid4())
	processed_videos = []
	errors = []

	try:
		# Establish WebSocket connection
		ws_url = f'ws://{COMFY_HOST}/ws?clientId={client_id}'
		print(f'worker-comfyui - Connecting to websocket: {ws_url}')
		ws = websocket.WebSocket()
		ws.connect(ws_url, timeout=10)
		print(f'worker-comfyui - Websocket connected')

		# Process each image in the batch
		for idx, image_data in enumerate(input_images):
			print(f'worker-comfyui - Processing image {idx + 1}/{len(input_images)}: {image_data.get("name")}')
			
			# Update workflow with current image name
			current_workflow = json.loads(json.dumps(workflow))
			
			# Find the LoadImage node and update it with current image name
			for node_id, node in current_workflow.items():
				if node.get('class_type') == 'LoadImage':
					node['inputs']['image'] = image_data.get('name')
					break

			result = process_single_image(current_workflow, image_data, job_id, client_id, ws)
			
			if result.get('success'):
				processed_videos.append({
					'original_name': image_data.get('name'),
					'output_filename': result.get('filename'),
					'data': result.get('data')
				})
				print(f'worker-comfyui - Successfully processed {image_data.get("name")}')
			else:
				error_msg = f"Failed to process {image_data.get('name')}: {result.get('error')}"
				errors.append(error_msg)
				print(f'worker-comfyui - {error_msg}')

		# Create a zip file with all videos
		if processed_videos:
			print(f'worker-comfyui - Creating zip file with {len(processed_videos)} videos')
			
			# Create temp directory for videos
			with tempfile.TemporaryDirectory() as temp_dir:
				zip_path = os.path.join(temp_dir, f'{batch_id}_videos.zip')
				
				with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
					for video in processed_videos:
						# Create a unique filename for each video
						original_name = os.path.splitext(video['original_name'])[0]
						output_ext = os.path.splitext(video['output_filename'])[1]
						video_filename = f'{original_name}_output{output_ext}'
						
						# Write video data to temp file then add to zip
						temp_video_path = os.path.join(temp_dir, video_filename)
						with open(temp_video_path, 'wb') as f:
							f.write(video['data'])
						
						zipf.write(temp_video_path, video_filename)
						print(f'worker-comfyui - Added {video_filename} to zip')

				# Upload zip file to S3 if configured, otherwise return as base64
				if os.environ.get('BUCKET_ENDPOINT_URL'):
					try:
						print(f'worker-comfyui - Uploading zip to S3...')
						s3_url = rp_upload.upload_image(job_id, zip_path)
						print(f'worker-comfyui - Uploaded zip to S3: {s3_url}')
						
						return {
							'status': 'success',
							'batch_id': batch_id,
							'total_images': len(input_images),
							'successful': len(processed_videos),
							'failed': len(errors),
							'zip_url': s3_url,
							'errors': errors if errors else None
						}
					except Exception as e:
						error_msg = f'Error uploading zip to S3: {e}'
						print(f'worker-comfyui - {error_msg}')
						errors.append(error_msg)
				else:
					# Return as base64
					try:
						with open(zip_path, 'rb') as f:
							zip_data = f.read()
						
						base64_zip = base64.b64encode(zip_data).decode('utf-8')
						print(f'worker-comfyui - Encoded zip as base64')
						
						return {
							'status': 'success',
							'batch_id': batch_id,
							'total_images': len(input_images),
							'successful': len(processed_videos),
							'failed': len(errors),
							'zip_data': base64_zip,
							'zip_filename': f'{batch_id}_videos.zip',
							'errors': errors if errors else None
						}
					except Exception as e:
						error_msg = f'Error encoding zip to base64: {e}'
						print(f'worker-comfyui - {error_msg}')
						errors.append(error_msg)

	except websocket.WebSocketException as e:
		print(f'worker-comfyui - WebSocket Error: {e}')
		print(traceback.format_exc())
		return {'error': f'WebSocket communication error: {e}'}
	except requests.RequestException as e:
		print(f'worker-comfyui - HTTP Request Error: {e}')
		print(traceback.format_exc())
		return {'error': f'HTTP communication error with ComfyUI: {e}'}
	except ValueError as e:
		print(f'worker-comfyui - Value Error: {e}')
		print(traceback.format_exc())
		return {'error': str(e)}
	except Exception as e:
		print(f'worker-comfyui - Unexpected Handler Error: {e}')
		print(traceback.format_exc())
		return {'error': f'An unexpected error occurred: {e}'}
	finally:
		if ws and ws.connected:
			print(f'worker-comfyui - Closing websocket connection.')
			ws.close()

	# If we get here, something went wrong
	return {
		'error': 'Batch processing failed',
		'details': errors,
		'successful': len(processed_videos),
		'failed': len(errors)
	}

if __name__ == '__main__':
	print('worker-comfyui - Starting handler...')
	runpod.serverless.start({'handler': handler})