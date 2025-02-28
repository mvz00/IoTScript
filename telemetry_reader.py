import random
import time
import uuid
import logging  # Add this import
from datetime import datetime, timezone
import serial
import serial.tools.list_ports
from threading import Thread, Event
import json
from common import setup_logging, get_config, get_active_buffer, write_lock
from pathlib import Path

logger = setup_logging("telemetry_reader", log_file_path=Path("log/reader.log"))
config = get_config()

def is_port_available(port_number):
    available_ports = [p.device for p in serial.tools.list_ports.comports()]
    return port_number in available_ports

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
            
            # Ensure buffer directory exists
            active_buffer.parent.mkdir(parents=True, exist_ok=True)
            
            with open(active_buffer, 'w') as f:
                json.dump(telemetry_data, f, indent=2)
            logger.info(f"Telemetry data written to buffer. Total records: {len(telemetry_data)}")
    except Exception as e:
        logger.error(f"Error writing telemetry to disk: {e}")

def read_telemetry_from_port(port_config, shutdown_event):
    port_number = port_config["gatewayPortId"]
    sensor_id = port_config["sensorId"]
    sensor_type_id = port_config["sensorTypeId"]
    sensor_type_code = port_config["sensorTypeCode"]
    sensor_position_id = port_config["sensorPositionId"]
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
            if logger.isEnabledFor(logging.DEBUG):  # Changed from logging.getLogger() to logger
                logger.debug(f"Starting telemetry read cycle for {port_number} (Simulation: {should_simulate})")

            value = None
            if should_simulate:
                logger.info(f"Simulating reading for {sensor_type_code} on port {port_number}")
                value = random.uniform(min_sim_value, max_sim_value)
                if logger.isEnabledFor(logging.DEBUG):  # Changed here too
                    logger.debug(f"Generated simulated value for {sensor_type_code}: {value}")
            else:
                logger.info(f"Attempting live reading from {sensor_type_code} on port {port_number}")
                
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
                                if logger.isEnabledFor(logging.DEBUG):  # Changed here too
                                    logger.debug(f"Read live value from {port_number}: {value}")
                            except ValueError as ve:
                                logger.error(f"Invalid value received from {port_number}: '{response}' - {ve}")
                        else:
                            logger.error(f"No response received from {port_number} for {sensor_type_code}")
                    finally:
                        ser.close()
                except Exception as e:
                    logger.error(f"Error accessing port {port_number}: {e}")

            if value is not None:
                telemetry_data = {
                    "readingGUID": str(uuid.uuid4()),
                    "sensorId": sensor_id,
                    "sensorTypeId": sensor_type_id,
                    "sensorTypeCode": sensor_type_code,
                    "sensorPositionId": sensor_position_id,
                    "gatewayPortId": port_number,
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

def main():
    shutdown_event = Event()
    threads = []
    
    try:
        logger.info("Starting telemetry collection...")
        
        for port_config in config["serialPorts"]:
            if port_config["active"]:
                thread = Thread(
                    target=read_telemetry_from_port,
                    args=(port_config, shutdown_event),
                    name=f"Port_{port_config['gatewayPortId']}"  # Changed from gatewayPortId to match config
                )
                thread.daemon = True
                thread.start()
                threads.append(thread)
                logger.info(f"Started thread for {port_config['sensorTypeCode']} on {port_config['gatewayPortId']}")
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down telemetry collection...")
        shutdown_event.set()
        for thread in threads:
            thread.join(timeout=5)
    
    logger.info("Telemetry collection stopped")

if __name__ == "__main__":
    main()
