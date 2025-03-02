# Conductivity Sensor Calibration Tool

This Python script provides a user-friendly interface for calibrating conductivity sensors using the Modbus RTU protocol over RS485-to-USB connection.

## Features

- Connect to conductivity sensors via RS485-to-USB adapter
- Read current temperature and conductivity values
- View and change standard solution settings
- Perform sensor calibration
- Reset device to factory defaults
- Modify device settings (address, baud rate)

## Requirements

- Python 3.6+
- pyserial library (`pip install pyserial`)
- RS485-to-USB converter
- Conductivity sensor with Modbus RTU protocol support

## Usage

1. Connect your RS485-to-USB converter to your computer
2. Connect the conductivity sensor to the RS485 converter
3. Run the script:
   ```
   python calibrate_conductivity.py
   ```
4. Follow the on-screen prompts to:
   - Select the correct serial port
   - Set the baud rate (default: 9600)
   - Set the device address (default: 4)

## Calibration Procedure

1. From the main menu, first check the current standard solution setting (option 3)
2. Set the appropriate standard solution type if needed (option 4)
   - 0: 84 µS/cm
   - 1: 1413 µS/cm
   - 2: 12.88 mS/cm
3. Ensure the electrode is clean and dry
4. Place the electrode in the selected standard solution
5. Wait for the reading to stabilize (check with option 2)
6. Select the calibration option (option 5) and confirm to complete the calibration

## Troubleshooting

- **No serial ports detected**: Check USB connections and ensure the RS485-to-USB converter is properly connected
- **Connection timeout**: Verify the baud rate and device address settings
- **Unexpected responses**: Ensure the device supports the Modbus RTU protocol and verify wiring

## Notes

- This tool is based on the specifications provided in the sensor manual
- Default device address is 4, but can be changed according to your configuration
- Communication parameters: 8N1 (8 data bits, no parity, 1 stop bit)
- Supports baud rates: 4800, 9600 (default), and 19200 