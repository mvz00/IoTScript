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
                        # Modbus read command parameters based on sensor type
                        if sensor_type_code == "TEMPERATURE":
                            device_address = port_config.get("temperatureDeviceAddress", 5)
                            function_code = port_config.get("temperatureFunctionCode", 3)
                            start_address = port_config.get("temperatureStartAddress", 0)
                            num_registers = port_config.get("temperatureNumRegisters", 1)
                        elif sensor_type_code == "CONDUCTIVITY":
                            device_address = port_config.get("conductivityDeviceAddress", 4)
                            function_code = port_config.get("conductivityFunctionCode", 3)
                            start_address = port_config.get("conductivityStartAddress", 1)
                            num_registers = port_config.get("conductivityNumRegisters", 2)
                        elif sensor_type_code == "PRESSURE":
                            device_address = port_config.get("pressureDeviceAddress", 4)
                            function_code = port_config.get("pressureFunctionCode", 3)
                            start_address = port_config.get("pressureStartAddress", 2)
                            num_registers = port_config.get("pressureNumRegisters", 1)
                        else:
                            logger.error(f"[MODBUS-{cycle_count}] Unknown sensor type code: {sensor_type_code}")
                            continue

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
                            
                            # Process response based on sensor type
                            if sensor_type_code == "TEMPERATURE":
                                temperature_hex = response[3:5]
                                temperature = int.from_bytes(temperature_hex, byteorder='big') * 0.1
                                value = temperature
                                logger.debug(f"[MODBUS-{cycle_count}] Parsed temperature value: {value} (raw hex: {temperature_hex.hex()})")
                            elif sensor_type_code == "CONDUCTIVITY":
                                conductivity_hex = response[3:7]
                                conductivity = int.from_bytes(conductivity_hex, byteorder='big') * 0.001
                                value = conductivity
                                logger.debug(f"[MODBUS-{cycle_count}] Parsed conductivity value: {value} (raw hex: {conductivity_hex.hex()})")
                            elif sensor_type_code == "PRESSURE":
                                pressure_hex = response[3:5]
                                pressure = int.from_bytes(pressure_hex, byteorder='big') * 0.1
                                value = pressure
                                logger.debug(f"[MODBUS-{cycle_count}] Parsed pressure value: {value} (raw hex: {pressure_hex.hex()})")
                            
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
                logger.info(f"[DATA-{cycle_count}] Writing telemetry data to disk: {json.dumps(telemetry_data)}")
                
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
