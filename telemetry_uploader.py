import time
import uuid
import logging
import sys
import traceback
from datetime import datetime, timezone
from azure.iot.device import IoTHubDeviceClient, Message, exceptions
import json
from threading import Event, Thread, Lock
import gzip
import shutil
import socket
from pathlib import Path
from common import (
    setup_logging, get_config, get_active_buffer, switch_buffer,
    write_lock, ARCHIVE_FILE_PATH
)

logger = setup_logging("telemetry_uploader", log_file_path=Path("log/writer.log"))
config = get_config()

# Global shutdown event that can be set from background threads
global_shutdown_event = Event()

# Client lock to prevent multiple connection issues
client_lock = Lock()

def sanitize_filename(timestamp):
    return timestamp.replace(':', '-').replace('+', '_plus_')

def archive_telemetry_data(buffer_file):
    try:
        archive_dir = Path(ARCHIVE_FILE_PATH)
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now(timezone.utc).isoformat()
        safe_timestamp = sanitize_filename(timestamp)
        archive_file = archive_dir / f"telemetry_{safe_timestamp}.json.gz"
        
        with gzip.open(archive_file, 'wb') as f_out:
            with open(buffer_file, 'rb') as f_in:
                shutil.copyfileobj(f_in, f_out)
        buffer_file.unlink(missing_ok=True)
        logger.info(f"Telemetry data archived to {archive_file}")
    except Exception as e:
        logger.error(f"Error archiving telemetry data: {e}")
        raise

def check_internet_connection():
    """Check if there is an active internet connection"""
    try:
        # Try to connect to a reliable server (Cloudflare DNS)
        socket.create_connection(("1.1.1.1", 53), timeout=5)
        return True
    except (socket.timeout, socket.error):
        return False

def handle_connection_state_change(connected):
    """Handle connection state changes"""
    if connected:
        logger.info("Device connected to IoT Hub")
    else:
        # Just log this, don't take any action
        logger.info("Device disconnected from IoT Hub")

def handle_background_exception(e):
    """Handle background exceptions from the IoT client"""
    # Just log the error, don't take any action
    logger.debug(f"Background exception (safe to ignore): {e}")

def create_client():
    """Create and configure an IoT Hub client with proper exception handlers"""
    # Create client
    client = IoTHubDeviceClient.create_from_connection_string(
        config["connectionString"]
    )
    
    # Register handlers, but don't log every background exception 
    # as these are expected during client lifecycle on Raspberry Pi
    client.on_background_exception = handle_background_exception
    client.on_connection_state_change = handle_connection_state_change
    
    return client

def safe_client_shutdown(client):
    """Safely shut down a client without exceptions"""
    if client is None:
        return
        
    try:
        if client.connected:
            client.disconnect()
    except:
        pass
        
    try:
        client.shutdown()
    except:
        pass
    
    logger.info("Client resources released")

def send_message_with_retry(telemetry_data, shutdown_event):
    """Send a single message with retry logic"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries and not shutdown_event.is_set():
        with client_lock:  # Ensure we don't have overlapping client operations
            client = None
            try:
                # Create a fresh client for each attempt
                client = create_client()
                
                # Connect to IoT Hub
                client.connect()
                logger.info("Connected to IoT Hub for message send")
                
                # Create the payload
                payload = {
                    "payloadGUID": str(uuid.uuid4()),
                    "gatewayId": config["gatewayId"],
                    "modelNumber": config["modelNumber"],
                    "serialNumber": config["serialNumber"],
                    "organisationId": config["organisationId"],
                    "siteId": config["siteId"],
                    "telemetry": telemetry_data,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                
                # Create and send the message
                telemetry_message = Message(json.dumps(payload))
                telemetry_message.content_type = "application/json"
                telemetry_message.content_encoding = "utf-8"
                
                logger.info(f"Sending telemetry message with {len(telemetry_data)} readings")
                # Send the message and wait for the result
                client.send_message(telemetry_message)
                logger.info("Message sent successfully")
                
                # Clean shutdown
                safe_client_shutdown(client)
                
                # Message sent successfully
                return True
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Error on send attempt {retry_count}: {str(e)}")
                
                # Clean up client resources
                safe_client_shutdown(client)
                
                if retry_count < max_retries:
                    # Wait with exponential backoff
                    wait_time = 5 * retry_count
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached. Failed to send message.")
    
    return False

def safe_send_telemetry(telemetry_data, shutdown_event):
    """Send telemetry with proper error handling and buffer management"""
    if not telemetry_data:
        logger.info("No telemetry data to send")
        return True
    
    try:
        # Check internet connection first
        if not check_internet_connection():
            logger.warning("No internet connection available. Will retry later.")
            return False
        
        # Send message with retry logic
        return send_message_with_retry(telemetry_data, shutdown_event)
        
    except Exception as e:
        logger.error(f"Unexpected error in safe_send_telemetry: {e}")
        return False

def main():
    shutdown_event = global_shutdown_event
    
    try:
        logger.info("Starting telemetry uploader")
        
        last_send_time = time.time()
        seconds_between_sends = config["secondsBetweenSends"]
        
        while not shutdown_event.is_set():
            current_time = time.time()
            if current_time - last_send_time >= seconds_between_sends:
                logger.info("Preparing to send telemetry data to IoT Hub")
                
                # Switch buffers atomically
                with write_lock:
                    send_buffer = get_active_buffer()
                    if send_buffer.exists():
                        new_buffer = switch_buffer()
                        # Initialize new buffer
                        with open(new_buffer, 'w') as f:
                            json.dump([], f)
                
                # Process the full buffer
                if send_buffer.exists():
                    try:
                        with open(send_buffer, 'r') as f:
                            telemetry_data = json.load(f)
                        logger.info(f"Read {len(telemetry_data)} records from buffer")
                        
                        # Send telemetry with our improved function
                        if telemetry_data:
                            success = safe_send_telemetry(telemetry_data, shutdown_event)
                            
                            # Handle the buffer based on success
                            if success:
                                if config["archiveTelemetry"]:
                                    archive_telemetry_data(send_buffer)
                                else:
                                    send_buffer.unlink(missing_ok=True)
                                    logger.info("Send buffer cleared")
                            else:
                                logger.warning("Failed to send telemetry. Buffer will be retained for next attempt.")
                    
                    except Exception as e:
                        logger.error(f"Error processing send buffer: {e}")
                
                last_send_time = current_time
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down uploader...")
        shutdown_event.set()
    except Exception as e:
        logger.critical(f"Unexpected error in main loop: {e}", exc_info=True)
        shutdown_event.set()

if __name__ == "__main__":
    main()
