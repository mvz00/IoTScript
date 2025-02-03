import random
import time
import logging
import uuid
import json
import platform
from datetime import datetime, timedelta
from azure.iot.device import IoTHubDeviceClient, Message
import os
from pathlib import Path
from threading import Timer
import serial  # Ensure pyserial is installed: pip install pyserial

# Load configuration
with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Azure IoT Hub connection string
CONNECTION_STRING = config["connectionString"]

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mode configuration
MODE = config["mode"]

# Log file configuration
LOG_FILE_PATH = Path(config["logFilePath"])
LOG_RETENTION_DAYS = config["logRetentionDays"]

# Telemetry data file configuration
TELEMETRY_FILE_PATH = Path(config["telemetryFilePath"])

# Initialize Azure IoT client
client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
logger.info("Azure IoT client initialized")

# Static values for the simulator
GATEWAY_ID = "RaspberryPi-Serial-12345"
CONDUCTIVITY_SENSOR_ID = "Conductivity-Sensor-67890"
TEMPERATURE_SENSOR_ID = "Temperature-Sensor-54321"
PRESSURE_SENSOR_ID = "Pressure-Sensor-09876"

# Serial port configuration based on OS
if platform.system() == "Windows":
    CONDUCTIVITY_PORT = config["serialPorts"]["conductivity"]
    TEMPERATURE_PORT = config["serialPorts"]["temperature"]
    PRESSURE_PORT = config["serialPorts"]["pressure"]
else:
    CONDUCTIVITY_PORT = "/dev/ttyUSB0"
    TEMPERATURE_PORT = "/dev/ttyUSB1"
    PRESSURE_PORT = "/dev/ttyUSB2"

def setup_logging():
    # Ensure the directory for the log file exists
    LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(LOG_FILE_PATH)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
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

def write_telemetry_to_disk(data, status):
    if TELEMETRY_FILE_PATH.exists():
        with open(TELEMETRY_FILE_PATH, 'r') as f:
            telemetry_data = json.load(f)
    else:
        telemetry_data = []

    telemetry_data.append({"data": data, "status": status})
    
    with open(TELEMETRY_FILE_PATH, 'w') as f:
        json.dump(telemetry_data, f, indent=2)

def send_telemetry_message(telemetry_data):
    telemetry_message = Message(str(telemetry_data))
    logger.info("Sending telemetry message to Azure IoT Hub")
    try:
        client.send_message(telemetry_message)
        logger.info("Telemetry message successfully sent")
        write_telemetry_to_disk(telemetry_data, "sent")
    except Exception as e:
        logger.error(f"Failed to send telemetry message: {e}")
        write_telemetry_to_disk(telemetry_data, "unsent")

def generate_probe_data():
    if MODE == "simulation":
        base_conductivity = 2.5  # Base value for conductivity in mS/cm
        base_temperature = 45.5  # Base value for temperature in °C
        base_pressure = 18.0  # Base value for pressure in bar

        conductivity = round(base_conductivity * random.uniform(0.95, 1.05), 2)
        temperature = round(base_temperature * random.uniform(0.95, 1.05), 1)
        pressure = round(base_pressure * random.uniform(0.95, 1.05), 2)
    else:
        # Replace with actual sensor data retrieval logic
        conductivity = get_live_conductivity()
        temperature = get_live_temperature()
        pressure = get_live_pressure()
    return conductivity, temperature, pressure

def get_live_conductivity():
    try:
        with serial.Serial(CONDUCTIVITY_PORT, 9600, timeout=1) as ser:
            ser.write(b'READ_CONDUCTIVITY\n')
            response = ser.readline().decode('utf-8').strip()
            conductivity = float(response)
            return conductivity
    except Exception as e:
        logger.error(f"Error reading conductivity: {e}")
        return None

def get_live_temperature():
    try:
        with serial.Serial(TEMPERATURE_PORT, 9600, timeout=1) as ser:
            ser.write(b'READ_TEMPERATURE\n')
            response = ser.readline().decode('utf-8').strip()
            temperature = float(response)
            return temperature
    except Exception as e:
        logger.error(f"Error reading temperature: {e}")
        return None

def get_live_pressure():
    try:
        with serial.Serial(PRESSURE_PORT, 9600, timeout=1) as ser:
            ser.write(b'READ_PRESSURE\n')
            response = ser.readline().decode('utf-8').strip()
            pressure = float(response)
            return pressure
    except Exception as e:
        logger.error(f"Error reading pressure: {e}")
        return None

try:
    setup_logging()
    while True:
        conductivity, temperature, pressure = generate_probe_data()
        
        # Combined payload
        telemetry_data = {
            "id": str(uuid.uuid4()),
            "gatewayId": GATEWAY_ID,
            "conductivity": conductivity,
            "temperature": temperature,
            "pressure": pressure,
            "timestamp": datetime.utcnow().isoformat()
        }
        send_telemetry_message(telemetry_data)
        
        time.sleep(5)
except KeyboardInterrupt:
    logger.info("Exiting program due to keyboard interrupt...")
finally:
    client.shutdown()
    logger.info("Azure IoT client shut down")