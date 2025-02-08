import random
import time
import logging
import uuid
import json
import platform
from datetime import datetime, timedelta, timezone
from azure.iot.device import IoTHubDeviceClient, Message, exceptions
import os
from pathlib import Path
from threading import Timer, Thread, Lock, Event
import serial
import gzip
import shutil
import serial.tools.list_ports  # Add this import at the top

# Load configuration
with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Azure IoT Hub connection string
CONNECTION_STRING = config["connectionString"]

# Add logging mode configuration
LOGGING_MODES = {
    "low": {
        "console_level": logging.WARNING,
        "file_level": logging.ERROR,
        "format": '%(asctime)s - %(levelname)s - %(message)s'
    },
    "standard": {
        "console_level": logging.INFO,
        "file_level": logging.INFO,
        "format": '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    },
    "verbose": {
        "console_level": logging.DEBUG,
        "file_level": logging.DEBUG,
        "format": '%(asctime)s - %(name)s - %(levelname)s - %(message)s - [%(threadName)s] %(funcName)s:%(lineno)d'
    }
}

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Log file configuration
LOG_FILE_PATH = Path(config["logFilePath"])
LOG_RETENTION_DAYS = config["logRetentionDays"]

# Telemetry data file configuration
TELEMETRY_FILE_PATH = Path(config["telemetryFilePath"])
ARCHIVE_FILE_PATH = Path(config["archiveFilePath"])
ARCHIVE_TELEMETRY = config["archiveTelemetry"]

# Add buffer configuration
BUFFER_A = Path(config["telemetryFilePath"]).parent / "buffer_a.json"
BUFFER_B = Path(config["telemetryFilePath"]).parent / "buffer_b.json"
ACTIVE_BUFFER_FILE = Path(config["telemetryFilePath"]).parent / "active_buffer.txt"

# Initialize locks
write_lock = Lock()
send_lock = Lock()

# Initialize Azure IoT client
client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
logger.info("Azure IoT client initialized")

# Device information
GATEWAY_ID = config["gatewayId"]
MODEL_NUMBER = config["modelNumber"]
SERIAL_NUMBER = config["serialNumber"]
ORGANISATION_ID = config["organisationId"]
SITE_ID = config["siteId"]

# Serial port configuration
SERIAL_PORTS = config["serialPorts"]

# Mode configuration
SIMULATION_MODE = config.get("simulationMode", False)

# Initialize file lock
file_lock = Lock()

# Add thread control
shutdown_event = Event()

def setup_logging():
    # Get logging configuration
    logging_mode = config.get("loggingMode", "standard").lower()
    if logging_mode not in LOGGING_MODES:
        logging_mode = "standard"
        logger.warning(f"Invalid logging mode specified, defaulting to {logging_mode}")
    
    log_config = LOGGING_MODES[logging_mode]
    
    # Configure root logger
    logging.basicConfig(level=log_config["console_level"])
    
    # Ensure the directory for the log file exists
    LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Setup file handler with appropriate level and format
    file_handler = logging.FileHandler(LOG_FILE_PATH)
    file_handler.setLevel(log_config["file_level"])
    formatter = logging.Formatter(log_config["format"])
    file_handler.setFormatter(formatter)
    
    # Setup console handler with appropriate level and format
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_config["console_level"])
    console_handler.setFormatter(formatter)
    
    # Configure logger
    logger.setLevel(min(log_config["console_level"], log_config["file_level"]))
    logger.handlers = []  # Clear existing handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logging initialized in {logging_mode} mode")
    
    if logging_mode == "verbose":
        logger.debug("Verbose logging enabled - additional debug information will be included")
    
    clear_old_logs()

def clear_old_logs():
    if LOG_FILE_PATH.exists():
        cutoff_time = datetime.now() - timedelta(days=LOG_RETENTION_DAYS)
        with open(LOG_FILE_PATH, 'r') as f:
            lines = f.readlines()
        with open(LOG_FILE_PATH, 'w') as f:
            for line in lines:
                log_time = datetime.strptime(line.split(' - ')[0], '%Y-%m-%d %H:%M:%S,%f')
                if log_time > cutoff_time:
                    f.write(line)

def is_port_available(port_number):
    """Check if a serial port is available"""
    available_ports = [p.device for p in serial.tools.list_ports.comports()]
    return port_number in available_ports

def read_telemetry_from_port(port_config):
    port_number = port_config["portNumber"]
    sensor_id = port_config["sensorId"]
    sensor_type_id = port_config["sensorTypeId"]
    sensor_type_code = port_config["sensorTypeCode"]
    seconds_between_reads = port_config["secondsBetweenReads"]
    is_active = port_config["active"]
    should_simulate = port_config["simulate"]
    min_sim_value = port_config["mininimumSimulationValue"]
    max_sim_value = port_config["maximumSimulationValue"]

    if not is_active:
        logger.info(f"Sensor {sensor_type_code} on port {port_number} is not active. Thread terminating.")
        return

    while not shutdown_event.is_set():
        try:
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logger.debug(f"Starting telemetry read cycle for {port_number} (Simulation: {should_simulate})")

            value = None
            if should_simulate:
                logger.info(f"Simulating reading for {sensor_type_code} on port {port_number}")
                value = random.uniform(min_sim_value, max_sim_value)
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logger.debug(f"Generated simulated value for {sensor_type_code}: {value}")
            else:
                logger.info(f"Attempting live reading from {sensor_type_code} on port {port_number}")
                
                # Check if port exists before trying to open it
                if not is_port_available(port_number):
                    logger.error(f"Port {port_number} is not available")
                    time.sleep(seconds_between_reads)
                    continue

                try:
                    ser = serial.Serial(
                        port=port_number,
                        baudrate=9600,
                        timeout=1,
                        write_timeout=1
                    )
                    try:
                        ser.write(f'READ_{sensor_type_code}\n'.encode())
                        response = ser.readline().decode('utf-8').strip()
                        if response:
                            try:
                                value = float(response)
                                if logging.getLogger().isEnabledFor(logging.DEBUG):
                                    logger.debug(f"Read live value from {port_number}: {value}")
                            except ValueError as ve:
                                logger.error(f"Invalid value received from {port_number}: '{response}' - {ve}")
                        else:
                            logger.error(f"No response received from {port_number} for {sensor_type_code}")
                    except serial.SerialTimeoutException as ste:
                        logger.error(f"Timeout writing to {port_number}: {ste}")
                    except serial.SerialException as se:
                        logger.error(f"Error communicating with {port_number}: {se}")
                    finally:
                        try:
                            ser.close()
                        except Exception as e:
                            logger.error(f"Error closing port {port_number}: {e}")
                except serial.SerialException as se:
                    logger.error(f"Could not open port {port_number}: {se}")
                except Exception as e:
                    logger.error(f"Unexpected error accessing {port_number}: {e}")

            if value is not None:
                telemetry_data = {
                    "sensorId": sensor_id,
                    "sensorTypeId": sensor_type_id,
                    "sensorTypeCode": sensor_type_code,
                    "portNumber": port_number,
                    "value": value,
                    "isSimulated": should_simulate,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                logger.info(f"Writing telemetry data to disk: {telemetry_data}")
                write_telemetry_to_disk(telemetry_data)
            else:
                logger.warning(f"No valid reading obtained for {sensor_type_code} on port {port_number}")

        except Exception as e:
            logger.error(f"Error in read_telemetry_from_port for {sensor_type_code}: {e}")
        
        shutdown_event.wait(seconds_between_reads)

def get_active_buffer():
    if not ACTIVE_BUFFER_FILE.exists():
        with open(ACTIVE_BUFFER_FILE, 'w') as f:
            f.write('A')
        return BUFFER_A
    with open(ACTIVE_BUFFER_FILE, 'r') as f:
        active = f.read().strip()
    return BUFFER_A if active == 'A' else BUFFER_B

def switch_buffer():
    with open(ACTIVE_BUFFER_FILE, 'r') as f:
        current = f.read().strip()
    with open(ACTIVE_BUFFER_FILE, 'w') as f:
        f.write('B' if current == 'A' else 'A')
    return BUFFER_B if current == 'A' else BUFFER_A

def write_telemetry_to_disk(data):
    try:
        active_buffer = get_active_buffer()
        with write_lock:
            telemetry_data = []
            if active_buffer.exists():
                try:
                    with open(active_buffer, 'r') as f:
                        telemetry_data = json.load(f)
                except (json.JSONDecodeError, FileNotFoundError):
                    telemetry_data = []

            telemetry_data.append(data)
            
            with open(active_buffer, 'w') as f:
                json.dump(telemetry_data, f, indent=2)
            logger.info(f"Telemetry data written to buffer. Total records: {len(telemetry_data)}")
    except Exception as e:
        logger.error(f"Error writing telemetry to disk: {e}")

def sanitize_filename(timestamp):
    """Sanitize timestamp for use in filename"""
    return timestamp.replace(':', '-').replace('+', '_plus_')

def ensure_archive_directory():
    """Ensure archive directory exists"""
    archive_dir = Path(ARCHIVE_FILE_PATH)
    archive_dir.mkdir(parents=True, exist_ok=True)
    return archive_dir

def archive_telemetry_data(buffer_file):
    try:
        archive_dir = ensure_archive_directory()
        timestamp = datetime.now(timezone.utc).isoformat()
        safe_timestamp = sanitize_filename(timestamp)
        archive_file = archive_dir / f"telemetry_{safe_timestamp}.json.gz"
        
        logger.info(f"Archiving telemetry to {archive_file}")
        with gzip.open(archive_file, 'wb') as f_out:
            with open(buffer_file, 'rb') as f_in:
                shutil.copyfileobj(f_in, f_out)
        buffer_file.unlink(missing_ok=True)
        logger.info(f"Telemetry data archived successfully")
    except Exception as e:
        logger.error(f"Error archiving telemetry data: {e}")
        raise

def send_telemetry_to_iot_hub():
    last_send_time = time.time()
    seconds_between_sends = config["secondsBetweenSends"]

    while not shutdown_event.is_set():
        try:
            current_time = time.time()
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logger.debug(f"Current time: {current_time}, Last send time: {last_send_time}")
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
                                "id": str(uuid.uuid4()),
                                "gatewayId": GATEWAY_ID,
                                "modelNumber": MODEL_NUMBER,
                                "serialNumber": SERIAL_NUMBER,
                                "organisationId": ORGANISATION_ID,
                                "siteId": SITE_ID,
                                "telemetry": telemetry_data,
                                "timestamp": datetime.now(timezone.utc).isoformat()
                            }
                            
                            if logging.getLogger().isEnabledFor(logging.DEBUG):
                                logger.debug(f"Payload prepared: {json.dumps(payload, indent=2)}")
                            
                            success = False
                            retry_count = 0
                            max_retries = 3
                            
                            while retry_count < max_retries and not success and not shutdown_event.is_set():
                                try:
                                    # Ensure client is connected
                                    if not client.connected:
                                        logger.info("Reconnecting to IoT Hub...")
                                        client.connect()
                                    
                                    telemetry_message = Message(json.dumps(payload))
                                    telemetry_message.content_type = "application/json"
                                    telemetry_message.content_encoding = "utf-8"
                                    
                                    logger.info(f"Attempt {retry_count + 1}/{max_retries} to send telemetry message with {len(telemetry_data)} readings")
                                    client.send_message(telemetry_message)
                                    
                                    # Verify the message was sent
                                    if client.connected:
                                        logger.info(f"Telemetry message successfully sent with {len(telemetry_data)} readings")
                                        success = True
                                        
                                        try:
                                            # Archive or delete the processed buffer
                                            if ARCHIVE_TELEMETRY:
                                                archive_telemetry_data(send_buffer)
                                            else:
                                                send_buffer.unlink(missing_ok=True)
                                                logger.info("Send buffer cleared")
                                        except Exception as e:
                                            logger.error(f"Error handling buffer after send: {e}")
                                    else:
                                        logger.error("Client disconnected after send attempt")
                                        
                                except exceptions.ConnectionDroppedError as e:
                                    retry_count += 1
                                    logger.error(f"Connection dropped (attempt {retry_count}/{max_retries}): {e}")
                                    time.sleep(2 ** retry_count)
                                    
                                except Exception as e:
                                    logger.error(f"Failed to send telemetry message: {e}")
                                    break
                            
                            if not success:
                                logger.error("Failed to send telemetry after all retries")
                    
                    except Exception as e:
                        logger.error(f"Error processing send buffer: {e}")
                
                last_send_time = time.time()
            
            time.sleep(0.1)
            
        except Exception as e:
            logger.error(f"Error in send_telemetry_to_iot_hub: {e}")
            time.sleep(1)

def main():
    setup_logging()
    threads = []
    
    try:
        logger.info("Starting telemetry collection...")
        
        # Ensure clean start
        if TELEMETRY_FILE_PATH.exists():
            TELEMETRY_FILE_PATH.unlink()
        
        # Start port reading threads
        for port_config in SERIAL_PORTS:
            if port_config["active"]:
                thread = Thread(target=read_telemetry_from_port, args=(port_config,), name=f"Port_{port_config['portNumber']}")
                thread.daemon = True
                thread.start()
                threads.append(thread)
                logger.info(f"Started thread for {port_config['sensorTypeCode']} on {port_config['portNumber']}")
        
        # Start send thread
        send_thread = Thread(target=send_telemetry_to_iot_hub, name="Sender")
        send_thread.daemon = True
        send_thread.start()
        threads.append(send_thread)
        logger.info("Started sender thread")

        # Monitor threads and keep main alive
        try:
            while True:
                alive_threads = [t for t in threads if t.is_alive()]
                if len(alive_threads) < len(threads):
                    dead_threads = [t.name for t in threads if not t.is_alive()]
                    logger.error(f"Dead threads detected: {dead_threads}")
                    break
                shutdown_event.wait(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, initiating graceful shutdown...")
            shutdown_event.set()
            
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        logger.info("Shutting down threads...")
        shutdown_event.set()
        
        # Wait for threads with timeout
        shutdown_timeout = 5
        shutdown_start = time.time()
        
        try:
            for thread in threads:
                remaining_time = max(0, shutdown_timeout - (time.time() - shutdown_start))
                if remaining_time > 0:
                    thread.join(timeout=remaining_time)
                if thread.is_alive():
                    logger.warning(f"Thread {thread.name} did not shutdown gracefully")
        except KeyboardInterrupt:
            logger.warning("Forced shutdown requested, some operations may not complete")
        
        try:
            client.shutdown()
            logger.info("Azure IoT client shut down")
        except Exception as e:
            logger.error(f"Error shutting down Azure IoT client: {e}")

if __name__ == "__main__":
    main()