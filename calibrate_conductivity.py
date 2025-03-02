#!/usr/bin/env python3
"""
Conductivity Sensor Calibration Tool

This script provides an interface to calibrate conductivity sensors using the Modbus RTU protocol.
Based on the sensor manual, it supports reading current values and performing calibration procedures.

Usage: python calibrate_conductivity.py
"""

import serial
import serial.tools.list_ports
import time
import struct
import argparse
import sys
from datetime import datetime


class ConductivityCalibrator:
    """Class for calibrating conductivity sensors via Modbus RTU protocol"""
    
    # Standard solution types
    STANDARD_SOLUTIONS = {
        0: "84 µS/cm",
        1: "1413 µS/cm",
        2: "12.88 mS/cm"
    }
    
    # Register addresses from the manual
    REGISTERS = {
        "temperature": 0,
        "conductivity": 1,
        "salinity": 2,
        "resistivity": 3,
        "tds": 4,
        "device_address": 5,
        "baud_rate": 6,
        "reset": 11,
        "factory_ec": 12,
        "calibration_cmd": 13,
        "standard_solution": 14,
        "temperature_compensation": 15,
        "tds_coefficient": 16,
        "reference_temperature": 17,
        "measurement_coef_x": 18,
        "measurement_coef_y": 19,
        "measurement_adjustment": 20,
        "temperature_adjustment": 21,
        "data_format": 22
    }
    
    def __init__(self, port=None, baudrate=9600, device_address=4):
        """Initialize the calibrator with connection settings"""
        self.port = port
        self.baudrate = baudrate
        self.device_address = device_address
        self.ser = None
        self.connected = False
    
    def list_available_ports(self):
        """List all available serial ports"""
        ports = serial.tools.list_ports.comports()
        print("\nAvailable serial ports:")
        for i, port in enumerate(ports):
            print(f"  {i+1}. {port.device} - {port.description}")
        return [p.device for p in ports]
    
    def connect(self):
        """Connect to the serial port"""
        try:
            self.ser = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=1,
                write_timeout=1
            )
            self.connected = True
            print(f"\nConnected to {self.port} at {self.baudrate} baud")
            return True
        except Exception as e:
            print(f"\nError connecting to {self.port}: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from the serial port"""
        if self.ser and self.ser.is_open:
            self.ser.close()
            self.connected = False
            print(f"\nDisconnected from {self.port}")
    
    def calculate_crc(self, data):
        """Calculate Modbus CRC16 for the given data"""
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
    
    def read_register(self, register_addr, count=1):
        """Read a register or registers using Modbus function code 03"""
        if not self.connected:
            print("Not connected to a device")
            return None
        
        # Build Modbus RTU command (function code 03 = read holding registers)
        command = bytes([
            self.device_address,  # Device address
            0x03,                 # Function code (03 = read holding registers)
            register_addr >> 8,   # Register address high byte
            register_addr & 0xFF, # Register address low byte
            0x00,                 # Number of registers high byte
            count                 # Number of registers low byte
        ])
        
        # Add CRC
        command += self.calculate_crc(command)
        
        try:
            # Send command
            self.ser.reset_input_buffer()
            self.ser.write(command)
            
            # Calculate expected response length: 
            # 1 byte address + 1 byte function code + 1 byte data length + data (2 bytes per register) + 2 bytes CRC
            expected_length = 5 + (2 * count)
            
            # Read response
            response = self.ser.read(expected_length)
            
            if len(response) != expected_length:
                print(f"Error: Expected {expected_length} bytes, got {len(response)}")
                return None
            
            # Check device address
            if response[0] != self.device_address:
                print(f"Error: Unexpected device address in response: {response[0]}")
                return None
            
            # Check function code
            if response[1] != 0x03:
                # Check if error response
                if response[1] == 0x83:
                    print(f"Error: Device returned error code: {response[2]}")
                else:
                    print(f"Error: Unexpected function code in response: {response[1]}")
                return None
            
            # Extract data
            data_length = response[2]
            data = response[3:3+data_length]
            
            # For single register, return as integer
            if count == 1:
                return (data[0] << 8) + data[1]
            
            # For multiple registers, return raw data for further processing
            return data
        
        except Exception as e:
            print(f"Error reading register: {e}")
            return None
    
    def write_register(self, register_addr, value):
        """Write a value to a register using Modbus function code 06"""
        if not self.connected:
            print("Not connected to a device")
            return False
        
        # Build Modbus RTU command (function code 06 = write single register)
        command = bytes([
            self.device_address,  # Device address
            0x06,                 # Function code (06 = write single register)
            register_addr >> 8,   # Register address high byte
            register_addr & 0xFF, # Register address low byte
            value >> 8,           # Value high byte
            value & 0xFF          # Value low byte
        ])
        
        # Add CRC
        command += self.calculate_crc(command)
        
        try:
            # Send command
            self.ser.reset_input_buffer()
            self.ser.write(command)
            
            # Read response (echo of the command if successful)
            response = self.ser.read(8)  # 8 bytes for write response
            
            if len(response) != 8:
                print(f"Error: Expected 8 bytes in response, got {len(response)}")
                return False
            
            # Check if response matches command (success)
            if response[:6] == command[:6]:
                return True
            else:
                print(f"Error: Unexpected response: {response.hex()}")
                return False
        
        except Exception as e:
            print(f"Error writing to register: {e}")
            return False
    
    def read_temperature(self):
        """Read the current temperature value"""
        value = self.read_register(self.REGISTERS["temperature"])
        if value is not None:
            # Apply scaling factor of 0.1 as per manual
            temperature = value * 0.1
            print(f"Current temperature: {temperature}°C")
            return temperature
        return None
    
    def read_conductivity(self):
        """Read the current conductivity value"""
        # According to the manual, conductivity may be stored in float format (ABCD)
        # Reading 2 registers to get the full 32-bit float
        value = self.read_register(self.REGISTERS["conductivity"], 2)
        if value is not None and len(value) == 4:
            # Convert 4 bytes to float (ABCD format / Big Endian)
            conductivity = struct.unpack('>f', value)[0]
            print(f"Current conductivity: {conductivity} µS/cm")
            return conductivity
        return None
    
    def read_standard_solution(self):
        """Read the current standard solution setting"""
        value = self.read_register(self.REGISTERS["standard_solution"])
        if value is not None:
            if value in self.STANDARD_SOLUTIONS:
                print(f"Current standard solution setting: {value} ({self.STANDARD_SOLUTIONS[value]})")
                return value
            else:
                print(f"Unknown standard solution value: {value}")
                return value
        return None
    
    def set_standard_solution(self, solution_type):
        """Set the standard solution type (0, 1, or 2)"""
        if solution_type not in self.STANDARD_SOLUTIONS:
            print(f"Invalid solution type. Must be one of: {list(self.STANDARD_SOLUTIONS.keys())}")
            return False
        
        if self.write_register(self.REGISTERS["standard_solution"], solution_type):
            print(f"Standard solution set to: {solution_type} ({self.STANDARD_SOLUTIONS[solution_type]})")
            return True
        return False
    
    def perform_calibration(self):
        """Perform the calibration procedure"""
        # According to manual, write 2 to register 13 to complete calibration
        if self.write_register(self.REGISTERS["calibration_cmd"], 2):
            print("Calibration command sent successfully")
            return True
        return False
    
    def reset_device(self):
        """Reset the device to factory defaults"""
        # According to manual, write 1524 to register 11 to reset
        if self.write_register(self.REGISTERS["reset"], 1524):
            print("Reset command sent successfully")
            return True
        return False


def main():
    """Main function to run the calibration tool"""
    print("=" * 60)
    print("Conductivity Sensor Calibration Tool")
    print("=" * 60)
    
    calibrator = ConductivityCalibrator()
    
    # List available ports
    available_ports = calibrator.list_available_ports()
    if not available_ports:
        print("No serial ports found. Please check connections and try again.")
        return
    
    # Get connection parameters from user
    try:
        port_selection = int(input("\nSelect a port number [1-{}]: ".format(len(available_ports))))
        if port_selection < 1 or port_selection > len(available_ports):
            print("Invalid selection.")
            return
        calibrator.port = available_ports[port_selection - 1]
    except ValueError:
        print("Invalid input. Please enter a number.")
        return
    
    try:
        baudrate = input(f"Enter baudrate [default: 9600]: ")
        if baudrate:
            calibrator.baudrate = int(baudrate)
    except ValueError:
        print("Invalid baudrate. Using default: 9600")
    
    try:
        device_address = input(f"Enter device address [default: 4]: ")
        if device_address:
            calibrator.device_address = int(device_address)
    except ValueError:
        print("Invalid device address. Using default: 4")
    
    # Connect to the device
    if not calibrator.connect():
        return
    
    try:
        while True:
            print("\n" + "=" * 60)
            print("MAIN MENU")
            print("=" * 60)
            print("1. Read current temperature")
            print("2. Read current conductivity")
            print("3. Read standard solution setting")
            print("4. Set standard solution")
            print("5. Perform calibration")
            print("6. Reset device")
            print("7. Change device settings")
            print("8. Exit")
            
            choice = input("\nEnter your choice [1-8]: ")
            
            if choice == '1':
                calibrator.read_temperature()
            
            elif choice == '2':
                calibrator.read_conductivity()
            
            elif choice == '3':
                calibrator.read_standard_solution()
            
            elif choice == '4':
                print("\nStandard Solution Types:")
                for key, value in calibrator.STANDARD_SOLUTIONS.items():
                    print(f"  {key}: {value}")
                
                try:
                    solution_type = int(input("\nEnter solution type [0-2]: "))
                    calibrator.set_standard_solution(solution_type)
                except ValueError:
                    print("Invalid input. Please enter a number.")
            
            elif choice == '5':
                print("\n" + "=" * 60)
                print("CALIBRATION PROCEDURE")
                print("=" * 60)
                print("1. Ensure the conductivity electrode is clean and dry")
                print("2. Verify the standard solution setting (option 3 in main menu)")
                print("3. Place the electrode in the corresponding standard solution")
                print("4. Wait for the reading to stabilize (check with option 2)")
                print("5. Proceed with calibration when ready")
                
                proceed = input("\nProceed with calibration? (y/n): ")
                if proceed.lower() == 'y':
                    calibrator.perform_calibration()
                else:
                    print("Calibration cancelled")
            
            elif choice == '6':
                confirm = input("\nWARNING: This will reset the device to factory defaults. Continue? (y/n): ")
                if confirm.lower() == 'y':
                    calibrator.reset_device()
                else:
                    print("Reset cancelled")
            
            elif choice == '7':
                print("\n" + "=" * 60)
                print("DEVICE SETTINGS")
                print("=" * 60)
                
                try:
                    new_addr = input("Enter new device address [current: {}] (leave empty to skip): ".format(calibrator.device_address))
                    if new_addr:
                        new_addr = int(new_addr)
                        if calibrator.write_register(calibrator.REGISTERS["device_address"], new_addr):
                            print(f"Device address changed to {new_addr}")
                            calibrator.device_address = new_addr
                        else:
                            print("Failed to change device address")
                    
                    # Baud rate options according to the manual
                    print("\nBaud rate options:")
                    print("  0: 4800")
                    print("  1: 9600")
                    print("  2: 19200")
                    
                    new_baud_option = input("Enter new baud rate option [leave empty to skip]: ")
                    if new_baud_option:
                        new_baud_option = int(new_baud_option)
                        if new_baud_option in [0, 1, 2]:
                            if calibrator.write_register(calibrator.REGISTERS["baud_rate"], new_baud_option):
                                print(f"Baud rate option changed to {new_baud_option}")
                                print("NOTE: Disconnect and reconnect with the new baud rate to continue")
                            else:
                                print("Failed to change baud rate")
                        else:
                            print("Invalid baud rate option")
                
                except ValueError:
                    print("Invalid input. Please enter valid numbers.")
            
            elif choice == '8':
                print("\nExiting program...")
                break
            
            else:
                print("\nInvalid choice. Please try again.")
    
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
    
    finally:
        calibrator.disconnect()


if __name__ == "__main__":
    main() 