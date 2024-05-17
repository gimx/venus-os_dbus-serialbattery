# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from battery import Battery, Cell
from utils import (
    is_bit_set,
    logger,
    MAX_BATTERY_CHARGE_CURRENT,
    MAX_BATTERY_DISCHARGE_CURRENT,
    MAX_CELL_VOLTAGE,
    MIN_CELL_VOLTAGE,
	UBMS_CAN_MODULE_SERIES,
    UBMS_CAN_MODULE_PARALLEL,
    zero_char,
)
from struct import unpack_from
import can
import time

"""
Data acquisition and decoding of Valence U-BMS messages on CAN bus

Majority of the protocol reverse engineering work was done by @cogito44 http://cogito44.free.fr
Original code dbus driver by @gimx https://github.com/gimx/dbus_ubms and 
Adapted to dbus serial battery by @gimx inspired by jkbms_can.py

To overcome the low resolution of the pack voltage reading(>=1V) the cell voltages are summed up.
In order for this to work the first x modules of a xSyP pack should have assigned module IDs 1 to x
The BMS should be operated in slave mode, VMU packages are being sent

# Restrictions seen from code:
-
"""


class Ubms_Can(Battery):
    def __init__(self, port, baud, address):
        super(Ubms_Can, self).__init__(port, baud, address)
        self._ci = False
        
        self.poll_interval = 1500
        self.type = self.BATTERYTYPE
        self.last_error_time = time.time()
        self.error_active = False
		
		self.maxChargeVoltage = voltage
		self.numberOfModules = UBMS_CAN_MODULE_SERIES * UBMS_CAN_MODULE_PARALLEL
		self.cell_count = self.numberOfModules * 4
        self.chargeComplete = 0
        self.soc = 0
        self.mode = 0
        self.state = ""
        self.voltage = 0
        self.current = 0
        self.temperature = 0
        self.balanced = True
        self.voltageAndCellTAlarms = 0
        self.internalErrors = 0
        self.currentAndPcbTAlarms = 0
        self.maxPcbTemperature = 0
        self.maxCellTemperature = 0
        self.minCellTemperature = 0
		
        self.cellVoltages =[(0,0,0,0) for i in range(self.numberOfModules)]
        self.moduleVoltage = [0 for i in range(self.numberOfModules)]
        self.moduleCurrent = [0 for i in range(self.numberOfModules)]
        self.moduleSoc = [0 for i in range(self.numberOfModules)]
        self.maxCellVoltage = 3.2
        self.minCellVoltage = 3.2
        self.maxChargeCurrent = MAX_BATTERY_CHARGE_CURRENT
        self.maxDischargeCurrent = MAX_BATTERY_DISCHARGE_CURRENT
        self.partnr = 0
        self.firmwareVersion = 'unknown'
        self.numberOfModulesBalancing = 0
        self.numberOfModulesCommunicating = 0
        self.updated = -1
        self.cyclicModeTask = None
	
		
		if self._ci is False:
            logger.debug("CAN bus init")
            # intit the can interface
            try:
                self._ci = can.interface.Bus(
                    bustype="socketcan", channel=self.port, bitrate=self.baud_rate,
					can_filters=[{"can_id": 0x0cf, "can_mask": 0xff0},
                                {"can_id": 0x350, "can_mask": 0xff0},
                                {"can_id": 0x360, "can_mask": 0xff0},
                                {"can_id": 0x46a, "can_mask": 0xff0},
                                {"can_id": 0x06a, "can_mask": 0xff0},
                        ])
                )
            except can.CanError as e:
                logger.error(e)

            if self._ci is None:
                return False

            logger.debug("CAN bus init done")

    def __del__(self):
        if self._ci:
			self._ci.notifier.stop()
            self._ci.shutdown()
            self._ci = False
            logger.debug("CAN bus shutdown")

    BATTERYTYPE = "Ubms_Can"
  
    def test_connection(self):
        # call a function that will connect to the battery, send a command and retrieve the result.
        # The result or call should be unique to this BMS. Battery name or version, etc.
        # Return True if success, False for failure
		
		# check connection and that reported system voltage roughly matches configuration
        found = False
        msg = None

        while True:
            try:
                msg = self._ci.recv(timeout=10)
				# listen for max 10s on the bus
            except can.CanE  rror:
                logger.error("CAN bus error")

            if msg == None:
            #timeout no system connected
                logger.error("No messages on CAN bus %s received. Check connection and speed." % self.port)
                break;

            elif msg.arbitration_id == 0xc1:
            # status message received, check pack voltage
                if abs(msg.data[0] - self.maxChargeVoltage) > 0.15* msg.data[0]:
                    logger.error("Pack voltage read (%dV) differs significantly from configured max charge voltage." % msg.data[0])
                else:
                    logger.info("Found Valence U-BMS")
                    found = True
                break;
				
			elif msg.arbitration_id == 0xc4:
            # status message received, check cell min/max voltages
				maxCellVoltage =  struct.unpack('<h', msg.data[4:6])[0]*0.001
                minCellVoltage =  struct.unpack('<h', msg.data[6:8])[0]*0.001
                if (maxCellVoltage < 4.5) and ( minCellVoltage > 2.0)
                    logger.info("Found Valence U-BMS")
                    found = True
                break;

        if found:
        # create a cyclic mode command message simulating a VMU master
        # a U-BMS in slave mode according to manual section 6.4.1 switches to standby
        # after 20 seconds of not receiving it
            msg = can.Message(arbitration_id=0x440,
                  data=[0, 2, 0, 0], is_extended_id=False) #default: drive mode
            
            self.cyclicModeTask = self._ci.send_periodic(msg, 1)
            notifier = can.Notifier(self._ci, [self])
    
        return found

    def get_settings(self):
        # After successful  connection get_settings will be call to set up the battery.
        # Set the current limits, populate cell count, etc
        # Return True if success, False for failure
        self.max_battery_charge_current = MAX_BATTERY_CHARGE_CURRENT
        self.max_battery_discharge_current = MAX_BATTERY_DISCHARGE_CURRENT
        self.max_battery_voltage = MAX_CELL_VOLTAGE * UBMS_CAN_MODULE_SERIES * 4
        self.min_battery_voltage = MIN_CELL_VOLTAGE * UBMS_CAN_MODULE_SERIES * 4

        # init the cell array add only missing Cell instances
        missing_instances = self.cell_count - len(self.cells)
        if missing_instances > 0:
            for c in range(missing_instances):
                self.cells.append(Cell(False))

        self.hardware_version = "U-BMS CAN " + str(self.numberOfModules) + " modules"
        return True

    def refresh_data(self):
        # call all functions that will refresh the battery data.
        # This will be called for every iteration (1 second)
        # Return True if success, False for failure
        result = self.read_status_data()

        return result

    def read_status_data(self):
        status_data = self.read_serial_data_ubms_CAN()
		
        # check if connection success
        if status_data is False:
            return False

        return True

    def to_protection_bits(self, byte_data):
        self.protection.voltage_cell_low =  (self.voltageAndCellTAlarms & 0x10)>>3
        self.protection.cell_overvoltage =  (self.voltageAndCellTAlarms & 0x20)>>4
        self.protection.soc_low = (self.voltageAndCellTAlarms & 0x08)>>3
        self._dbusservice['/Alarms/HighDischargeCurrent'] = (self.currentAndPcbTAlarms & 0x3)

#       flag high cell temperature alarm and high pcb temperature alarm
        self._dbusservice['/Alarms/HighTemperature'] =  (self.voltageAndCellTAlarms &0x6)>>1 | (self.currentAndPcbTAlarms & 0x18)>>3
        self._dbusservice['/Alarms/LowTemperature'] = (self.mode & 0x60)>>5

    def reset_protection_bits(self):
        self.protection.cell_overvoltage = 0
        self.protection.voltage_cell_low = 0
        self.protection.voltage_high = 0
        self.protection.voltage_low = 0
        self.protection.cell_imbalance = 0
        self.protection.current_under = 0
        self.protection.current_over = 0
		
        self.protection.temp_high_charge = 0
        self.protection.temp_high_discharge = 0
        self.protection.temp_low_charge = 0
        self.protection.temp_low_discharge = 0
        
        self.protection.soc_low = 0
        self.protection.internal_failure = 0

    def read_serial_data_ubms_CAN(self):
        
        # reset errors after timeout
        if ((time.time() - self.last_error_time) > 120.0) and self.error_active is True:
            self.error_active = False
            self.reset_protection_bits()

        # read msgs until we get one we want
        messages_to_read = self.MESSAGES_TO_READ
        while messages_to_read > 0:
            msg = self._ci.recv(1)
            if msg is None:
                logger.info("No CAN Message received")
                return False

            if msg is not None:
                # print("message received")
                messages_to_read -= 1
                # print(messages_to_read)
                if msg.arbitration_id in self.CAN_FRAMES[self.BATT_STAT]:
                    voltage = unpack_from("<H", bytes([msg.data[0], msg.data[1]]))[0]
                    self.voltage = voltage / 10

                    current = unpack_from("<H", bytes([msg.data[2], msg.data[3]]))[0]
                    self.current = (current / 10) - 400

                    self.soc = unpack_from("<B", bytes([msg.data[4]]))[0]

                    self.time_to_go = (
                        unpack_from("<H", bytes([msg.data[6], msg.data[7]]))[0] * 36
                    )

                    # print(self.voltage)
                    # print(self.current)
                    # print(self.soc)
                    # print(self.time_to_go)

                elif msg.arbitration_id in self.CAN_FRAMES[self.CELL_VOLT]:
                    max_cell_volt = (
                        unpack_from("<H", bytes([msg.data[0], msg.data[1]]))[0] / 1000
                    )
                    max_cell_nr = unpack_from("<B", bytes([msg.data[2]]))[0]
                    max_cell_cnt = max(max_cell_nr, self.cell_count)

                    min_cell_volt = (
                        unpack_from("<H", bytes([msg.data[3], msg.data[4]]))[0] / 1000
                    )
                    min_cell_nr = unpack_from("<B", bytes([msg.data[5]]))[0]
                    max_cell_cnt = max(min_cell_nr, max_cell_cnt)

                    if max_cell_cnt > self.cell_count:
                        self.cell_count = max_cell_cnt
                        self.get_settings()

                    for c_nr in range(len(self.cells)):
                        self.cells[c_nr].balance = False

                    if self.cell_count == len(self.cells):
                        self.cells[max_cell_nr - 1].voltage = max_cell_volt
                        self.cells[max_cell_nr - 1].balance = True

                        self.cells[min_cell_nr - 1].voltage = min_cell_volt
                        self.cells[min_cell_nr - 1].balance = True

                elif msg.arbitration_id in self.CAN_FRAMES[self.CELL_TEMP]:
                    max_temp = unpack_from("<B", bytes([msg.data[0]]))[0] - 50
                    min_temp = unpack_from("<B", bytes([msg.data[2]]))[0] - 50
                    self.to_temp(1, max_temp if max_temp <= 100 else 100)
                    self.to_temp(2, min_temp if min_temp <= 100 else 100)
                    # print(max_temp)
                    # print(min_temp)
                elif msg.arbitration_id in self.CAN_FRAMES[self.ALM_INFO]:
                    alarms = unpack_from(
                        "<L",
                        bytes([msg.data[0], msg.data[1], msg.data[2], msg.data[3]]),
                    )[0]
                    print("alarms %d" % (alarms))
                    self.last_error_time = time.time()
                    self.error_active = True
                    self.to_protection_bits(alarms)
        return True
		
	
    def on_message_received(self, msg):
        self.updated = msg.timestamp
        if msg.arbitration_id == 0xc0:
            self.soc = msg.data[0]
            self.mode = msg.data[1]
            self.state = self.opState[self.mode & 0xc]
			
			#self.balancing = True if (msg.data[1] & 0x10) > 0 # inter-module balancing, see numberOfModulesBalancing
           	self.protection.self.temp_low_charge_charge = WARNING if (msg.data[1] & 0x20) > 0 # cell temperature min warn
			self.protection.self.temp_low_charge_charge = ALARM   if (msg.data[1] & 0x40) > 0 # cell temperature min alarm
			self.protection.self.temp_low_charge_charge = SHUTDWN if (msg.data[1] & 0x80) > 0 # cell temperature min shutdown
			
			self.protection.internal_failure      = WARNING if (msg.data[2] & 0x01) > 0 # module not communicating
			self.protection.self.temp_high_charge = WARNING if (msg.data[2] & 0x02) > 0 # cell temperature max warn
			self.protection.self.temp_high_charge = ALARM   if (msg.data[2] & 0x04) > 0 # cell temperature max alarm
			self.protection.soc_low               = WARNING if (msg.data[2] & 0x08) > 0 # SOC below BMS configured limit
			self.protection.voltage_cell_low      = ALARM   if (msg.data[2] & 0x10) > 0 # cell voltage min alarm
		    self.protection.cell_overvoltage      = ALARM   if (msg.data[2] & 0x20) > 0 # cell voltage max alarm
			#self.protection.internal_failure      =  WARNING if (msg.data[2] & 0x40) > 0 # unused
			self.protection.internal_failure      =  ALARM if (msg.data[2] & 0x80) > 0 # cell temperature max shutdown
			
			#self.protection.internal_failure      = WARNING if (msg.data[3] & 0x01) > 0 # unused
			self.protection.internal_failure      = WARNING if (msg.data[3] & 0x02) > 0 # more modules than configured
			self.protection.internal_failure      = WARNING if (msg.data[3] & 0x04) > 0 # temperature sensor failure
			self.protection.internal_failure      = WARNING if (msg.data[3] & 0x08) > 0 # voltage sensor failure
			self.protection.internal_failure      = WARNING if (msg.data[3] & 0x10) > 0 # current sensor failure
			self.protection.internal_failure      = WARNING if (msg.data[3] & 0x20) > 0 # SOC mismatch between modules
			self.protection.voltage_cell_low      = WARNING if (msg.data[3] & 0x40) > 0 # cell voltage min warn
		    self.protection.cell_overvoltage      = WARNING if (msg.data[3] & 0x80) > 0 # cell voltage max warn

            self.protection.current_over          = WARNING if (msg.data[4] & 0x01) > 0 # over current warn
			self.protection.current_over          = ALARM   if (msg.data[4] & 0x02) > 0 # over current alarm
			self.protection.current_over          = SHUTDWN if (msg.data[4] & 0x04) > 0 # over current shutdown
			self.protection.internal_failure      = WARNING if (msg.data[4] & 0x08) > 0 # PCB temperature warn
			self.protection.internal_failure      = ALARM if (msg.data[4] & 0x10) > 0 # PCB temperature alarm
			self.protection.internal_failure      = SHUTDWN if (msg.data[4] & 0x20) > 0 # PCB temperature shutdown
			#self.protection.internal_failure      = WARNING if (msg.data[4] & 0x40) > 0 # unused
		    #self.protection.internal_failure      = WARNING if (msg.data[4] & 0x80) > 0 # unused

            self.numberOfModulesCommunicating = msg.data[5]
			self.numberOfModulesBalancing = msg.data[6]

		    #self.protection.internal_failure      = WARNING if (msg.data[7] & 0x01) > 0 # unused
			#self.protection.internal_failure      = WARNING if (msg.data[7] & 0x02) > 0 # unused
			self.protection.voltage_cell_low      = SHUTDWN if (msg.data[7] & 0x04) > 0 # cell voltage min shutdown
			self.protection.cell_overvoltage      = SHUTDWN if (msg.data[7] & 0x08) > 0 # cell voltage max shutdown
			self.protection.internal_failure      = WARNING if (msg.data[7] & 0x10) > 0 # Vehicule Management Unit (VMU) timeout
			self.protection.internal_failure      = WARNING if (msg.data[7] & 0x20) > 0 # drive contactor state change failure (stuck)
			self.protection.internal_failure      = WARNING if (msg.data[7] & 0x40) > 0 # sanity error
		    self.protection.internal_failure      = WARNING if (msg.data[7] & 0x80) > 0 # over voltage protection failure

            #if no module flagged missing and not too many on the bus, then this is the number the U-BMS was configured for
            if (msg.data[2] & 1 == 0) and (msg.data[3] & 2 == 0):
                self.numberOfModules = self.numberOfModulesCommunicating

            
        elif msg.arbitration_id == 0xc1:
#                self.voltage = msg.data[0] * 1 # voltage scale factor depends on BMS configuration!
            self.current = struct.unpack('Bb',msg.data[0:2])[1]

            if (self.mode & 0x2) != 0 : #provided in drive mode only
                self.maxDischargeCurrent =  int((struct.unpack('<h', msg.data[3:5])[0])/10)
                self.maxChargeCurrent =  int((struct.unpack('<h', bytearray([msg.data[5],msg.data[7]]))[0])/10)
                logger.debug("Icmax %dA Idmax %dA", self.maxChargeCurrent, self.maxDischargeCurrent)

            logger.debug("I: %dA U: %dV",self.current, self.voltage)

        elif msg.arbitration_id == 0xc2:
            #charge mode only
            if (self.mode & 0x1) != 0:
                self.chargeComplete = (msg.data[3] & 0x4) >> 2
                self.maxChargeVoltage2 = struct.unpack('<h', msg.data[1:3])[0]

                #only apply lower charge current when equalizing
                if (self.mode & 0x18) == 0x18 :
                    self.maxChargeCurrent = msg.data[0]
                else:
                #allow charge with 0.1C
                    self.maxChargeCurrent = self.capacity * 0.1

        elif msg.arbitration_id == 0xc4:
            self.maxCellTemperature =  msg.data[0]-40
            self.minCellTemperature =  msg.data[1]-40
            self.maxPcbTemperature =  msg.data[3]-40
            self.maxCellVoltage =  struct.unpack('<h', msg.data[4:6])[0]*0.001
            self.minCellVoltage =  struct.unpack('<h', msg.data[6:8])[0]*0.001
            logger.debug("Umin %1.3fV Umax %1.3fV", self.minCellVoltage, self.maxCellVoltage)

        elif msg.arbitration_id in [0x350, 0x352, 0x354, 0x356, 0x358, 0x35A, 0x35C, 0x35E, 0x360, 0x362, 0x364]:
            module = (msg.arbitration_id - 0x350) >> 1
            self.cellVoltages[module] = struct.unpack('>hhh', msg.data[2:msg.dlc])

        elif msg.arbitration_id in [0x351, 0x353, 0x355, 0x357, 0x359, 0x35B, 0x35D, 0x35F, 0x361, 0x363, 0x365]:
            module = (msg.arbitration_id - 0x351) >> 1
            self.cellVoltages[module] = self.cellVoltages[module]+ tuple(struct.unpack('>h', msg.data[2:msg.dlc]))
            self.moduleVoltage[module] = sum(self.cellVoltages[module])
            logger.debug("Umodule %d: %fmV", module, self.moduleVoltage[module])

            #update pack voltage at each arrival of the last modules cell voltages
            if module == self.numberOfModules-1:
                self.voltage = sum(self.moduleVoltage[0:3])/1000.0 #adjust slice to number of modules in series

        elif msg.arbitration_id in [0x46a, 0x46b, 0x46c, 0x46d]:
            iStart = (msg.arbitration_id - 0x46a) * 3
            fmt = '>' +'h' * int((msg.dlc - 2)/2)
            mCurrent = struct.unpack(fmt, msg.data[2:msg.dlc])
            self.moduleCurrent[iStart:] = mCurrent
            logger.debug("Imodule %s", ",".join(str(x) for x in self.moduleCurrent))

        elif msg.arbitration_id in [0x6a, 0x6b]:
            iStart = (msg.arbitration_id - 0x6a) * 7
            fmt = 'B' * (msg.dlc - 1)
            mSoc = struct.unpack(fmt, msg.data[1:msg.dlc])
            self.moduleSoc[iStart:] = tuple((m * 100)>>8 for m in mSoc)
            logger.debug("SOCmodule %s", ",".join(str(x) for x in self.moduleSoc))
