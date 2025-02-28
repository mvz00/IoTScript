import time
import uuid
from datetime import datetime, timezone
from azure.iot.device import IoTHubDeviceClient, Message, exceptions
import json
from threading import Event
import gzip
import shutil
from pathlib import Path
from common import (
    setup_logging, get_config, get_active_buffer, switch_buffer,
    write_lock, ARCHIVE_FILE_PATH
)

logger = setup_logging("telemetry_uploader", log_file_path=Path("log/writer.log"))
config = get_config()

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

def main():
    shutdown_event = Event()
    
    try:
        # Initialize Azure IoT client
        client = IoTHubDeviceClient.create_from_connection_string(config["connectionString"])
        logger.info("Azure IoT client initialized")
        
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
                        
                        if telemetry_data:
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
                            
                            success = False
                            retry_count = 0
                            max_retries = 3
                            
                            while retry_count < max_retries and not success and not shutdown_event.is_set():
                                try:
                                    if not client.connected:
                                        logger.info("Reconnecting to IoT Hub...")
                                        client.connect()
                                    
                                    telemetry_message = Message(json.dumps(payload))
                                    telemetry_message.content_type = "application/json"
                                    telemetry_message.content_encoding = "utf-8"
                                    
                                    logger.info(f"Sending telemetry message with {len(telemetry_data)} readings")
                                    client.send_message(telemetry_message)
                                    success = True
                                    
                                    # Archive or delete the processed buffer
                                    if config["archiveTelemetry"]:
                                        archive_telemetry_data(send_buffer)
                                    else:
                                        send_buffer.unlink(missing_ok=True)
                                        logger.info("Send buffer cleared")
                                        
                                except exceptions.ConnectionDroppedError as e:
                                    retry_count += 1
                                    logger.error(f"Connection dropped (attempt {retry_count}/{max_retries}): {e}")
                                    time.sleep(2 ** retry_count)
                                    
                                except Exception as e:
                                    logger.error(f"Failed to send telemetry message: {e}")
                                    break
                            
                    except Exception as e:
                        logger.error(f"Error processing send buffer: {e}")
                
                last_send_time = current_time
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down uploader...")
        shutdown_event.set()
    finally:
        try:
            client.shutdown()
            logger.info("Azure IoT client shut down")
        except Exception as e:
            logger.error(f"Error shutting down Azure IoT client: {e}")

if __name__ == "__main__":
    main()
