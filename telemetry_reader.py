import random
import time
import uuid
import logging  # Add this import
from datetime import datetime, timezone
import serial
import serial.tools.list_ports
from threading import Thread, Event, Lock
import json
from common import setup_logging, get_config, get_active_buffer, write_lock, get_tracked_guids, save_tracked_guids
from pathlib import Path

logger = setup_logging("telemetry_reader", log_file_path=Path("log/reader.log"))
config = get_config()

# Track telemetry GUIDs to ensure uniqueness
telemetry_guid_lock = Lock()
tracking_data = get_tracked_guids()
telemetry_guids = set(tracking_data["reading_guids"])
MAX_GUID_CACHE_SIZE = 10000  # Prevent memory growth

logger.info(f"Loaded {len(telemetry_guids)} previously tracked reading GUIDs")

def is_port_available(port_number):
    available_ports = [p.device for p in serial.tools.list_ports.comports()]
    return port_number in available_ports

def generate_unique_reading_guid():
    """Generate a unique reading GUID that hasn't been used before"""
    with telemetry_guid_lock:
        reading_guid = str(uuid.uuid4())
        
        # Extremely unlikely, but check if this GUID has been used before
        while reading_guid in telemetry_guids:
            reading_guid = str(uuid.uuid4())
        
        # Add to the tracking set
        telemetry_guids.add(reading_guid)
        
        # Keep set size in check
        if len(telemetry_guids) > MAX_GUID_CACHE_SIZE:
            # Convert to list, trim, convert back to set to remove oldest entries
            guid_list = list(telemetry_guids)
            telemetry_guids.clear()
            telemetry_guids.update(guid_list[-MAX_GUID_CACHE_SIZE:])
        
        # Update the persistent tracking file
        global tracking_data
        tracking_data["reading_guids"] = list(telemetry_guids)
        save_tracked_guids(tracking_data)
        
        return reading_guid

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
            
            # Ensure data has a readingGUID if not already present
            if "readingGUID" not in data:
                data["readingGUID"] = generate_unique_reading_guid()
            
            telemetry_data.append(data)
            
            # Ensure buffer directory exists
            active_buffer.parent.mkdir(parents=True, exist_ok=True)
            
            with open(active_buffer, 'w') as f:
                json.dump(telemetry_data, f, indent=2)
            logger.info(f"Telemetry data written to buffer. Total records: {len(telemetry_data)}")
    except Exception as e:
        logger.error(f"Error writing telemetry to disk: {e}")

def calculate_crc(data):
    crc = 0xFFFF
    for pos in data:
        crc ^= pos
        for _ in range(8):
            if (crc & 0x0001) != 0:
                crc >>= 1
                crc ^= 0xA001
            else:
                crc >>= 1
    return crc.to_bytes(2, byteorder='little')

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

    logger.info(f"[INIT] Sensor thread initialized - Port: {port_number}, SensorID: {sensor_id}, Type: {sensor_type_code}, Position: {sensor_position_id}")
    logger.info(f"[INIT] Configuration - Active: {is_active}, Simulate: {should_simulate}, Read Interval: {seconds_between_reads}s")
    
    if not is_active:
        logger.info(f"[SHUTDOWN] Sensor {sensor_type_code} on port {port_number} is not active. Thread terminating.")
        return

    cycle_count = 0
    
    while not shutdown_event.is_set():
        cycle_start_time = time.time()
        cycle_count += 1
        
        try:
            logger.info(f"[CYCLE-{cycle_count}] Starting telemetry read cycle for {sensor_type_code} on port {port_number}")
            logger.debug(f"[CYCLE-{cycle_count}] Details - Simulation: {should_simulate}, SimRange: [{min_sim_value}-{max_sim_value}]")

            value = None
            if should_simulate:
                logger.info(f"[SIM-{cycle_count}] Simulating reading for {sensor_type_code} on port {port_number}")
                value = random.uniform(min_sim_value, max_sim_value)
                logger.info(f"[SIM-{cycle_count}] Generated simulated value: {value}")
            else:
                logger.info(f"[LIVE-{cycle_count}] Attempting live reading from {sensor_type_code} on port {port_number}")
                
                if not is_port_available(port_number):
                    logger.error(f"[LIVE-{cycle_count}] Port {port_number} is not available. Available ports: {[p.device for p in serial.tools.list_ports.comports()]}")
                    time.sleep(seconds_between_reads)
                    continue

                try:
                    logger.debug(f"[SERIAL-{cycle_count}] Opening serial port {port_number}")
                    serial_start_time = time.time()
                    
                    # Log serial port configuration
                    baud_rate = port_config.get("baudRate", 9600)
                    data_bits = port_config.get("dataBits", 8)
                    parity = port_config.get("parity", "None")
                    stop_bits = port_config.get("stopBits", 1)
                    
                    logger.debug(f"[SERIAL-{cycle_count}] Configuration - Baud: {baud_rate}, DataBits: {data_bits}, Parity: {parity}, StopBits: {stop_bits}")
                    
                    ser = serial.Serial(
                        port=port_number,
                        baudrate=baud_rate,
                        bytesize=serial.EIGHTBITS if data_bits == 8 else serial.SEVENBITS,
                        parity=serial.PARITY_NONE if parity == "None" else serial.PARITY_EVEN,
                        stopbits=serial.STOPBITS_ONE if stop_bits == 1 else serial.STOPBITS_TWO,
                        timeout=1,
                        write_timeout=1
                    )
                    logger.debug(f"[SERIAL-{cycle_count}] Serial port {port_number} opened successfully")
                    
                    try:
                        # Modbus read command parameters - now consistent across all sensor types
                        device_address = port_config.get("deviceAddress", 1)
                        function_code = port_config.get("functionCode", 3)
                        start_address = port_config.get("startAddress", 0)
                        num_registers = port_config.get("numRegisters", 1)

                        logger.debug(f"[MODBUS-{cycle_count}] Parameters - Device: 0x{device_address:02X}, Function: 0x{function_code:02X}, Address: 0x{start_address:04X}, Registers: {num_registers}")
                        
                        # Build and send Modbus command
                        command = f'{device_address:02X}{function_code:02X}{start_address:04X}{num_registers:04X}'
                        command_bytes = bytes.fromhex(command)
                        crc = calculate_crc(command_bytes)
                        full_command = command_bytes + crc
                        
                        logger.debug(f"[MODBUS-{cycle_count}] Sending command: {full_command.hex()}")
                        ser.write(full_command)
                        
                        # Read response
                        expected_response_length = 5 + 2 * num_registers
                        logger.debug(f"[MODBUS-{cycle_count}] Waiting for response, expected length: {expected_response_length} bytes")
                        
                        response = ser.read(expected_response_length)
                        
                        if response:
                            logger.debug(f"[MODBUS-{cycle_count}] Received raw response: {response.hex()}")
                            
                            # Process response - standardized for all sensor types
                            # Get data bytes based on number of registers
                            data_bytes = response[3:3 + num_registers * 2]
                            
                            # Convert data to a value - use a standardized scaling factor of 0.1
                            # This can be adjusted based on your specific needs or added as a config parameter
                            raw_value = int.from_bytes(data_bytes, byteorder='big')
                            value = raw_value * 0.1
                            
                            logger.debug(f"[MODBUS-{cycle_count}] Parsed value: {value} (raw hex: {data_bytes.hex()})")
                            
                            serial_end_time = time.time()
                            logger.debug(f"[SERIAL-{cycle_count}] Serial communication completed in {serial_end_time - serial_start_time:.3f} seconds")
                        else:
                            logger.error(f"[MODBUS-{cycle_count}] No response received from {port_number} for {sensor_type_code}")
                            # Log device status if possible
                            if ser.in_waiting:
                                logger.debug(f"[MODBUS-{cycle_count}] Bytes waiting in buffer: {ser.in_waiting}")
                                remaining_data = ser.read(ser.in_waiting)
                                if remaining_data:
                                    logger.debug(f"[MODBUS-{cycle_count}] Remaining data in buffer: {remaining_data.hex()}")
                    finally:
                        logger.debug(f"[SERIAL-{cycle_count}] Closing serial port {port_number}")
                        ser.close()
                except Exception as e:
                    import traceback
                    logger.error(f"[SERIAL-{cycle_count}] Error accessing port {port_number}: {e}")
                    logger.error(f"[SERIAL-{cycle_count}] Exception traceback: {traceback.format_exc()}")

            if value is not None:
                # Generate a unique GUID for this reading - it will persist even if the data is sent multiple times
                reading_guid = generate_unique_reading_guid()
                
                telemetry_data = {
                    "readingGUID": reading_guid,
                    "sensorId": sensor_id,
                    "sensorTypeId": sensor_type_id,
                    "sensorTypeCode": sensor_type_code,
                    "sensorPositionId": sensor_position_id,
                    "gatewayPortId": port_number,
                    "value": value,
                    "isSimulated": should_simulate,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                logger.info(f"[DATA-{cycle_count}] Writing telemetry data to disk with readingGUID: {reading_guid}")
                
                storage_start_time = time.time()
                write_telemetry_to_disk(telemetry_data)
                storage_end_time = time.time()
                
                logger.debug(f"[DATA-{cycle_count}] Data storage completed in {storage_end_time - storage_start_time:.3f} seconds")
            else:
                logger.warning(f"[DATA-{cycle_count}] No valid reading obtained for {sensor_type_code} on port {port_number}")

        except Exception as e:
            import traceback
            logger.error(f"[ERROR-{cycle_count}] Error in read_telemetry_from_port for {sensor_type_code} on port {port_number}: {e}")
            logger.error(f"[ERROR-{cycle_count}] Exception traceback: {traceback.format_exc()}")
        
        # Calculate and log cycle timing information
        cycle_end_time = time.time()
        cycle_duration = cycle_end_time - cycle_start_time
        logger.info(f"[TIMING-{cycle_count}] Cycle completed in {cycle_duration:.3f} seconds")
        
        # Calculate wait time (adjust if the cycle took longer than expected)
        wait_time = max(0.1, seconds_between_reads - cycle_duration)
        logger.debug(f"[TIMING-{cycle_count}] Waiting {wait_time:.3f} seconds until next cycle")
        
        shutdown_event.wait(wait_time)
    
    logger.info(f"[SHUTDOWN] Telemetry reader thread for {sensor_type_code} on port {port_number} shutting down after {cycle_count} cycles")

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
