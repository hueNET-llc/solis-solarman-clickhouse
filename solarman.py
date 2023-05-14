import aiochclient
import aiohttp
import asyncio
import colorlog
import datetime
import json
import logging
import umodbus.exceptions
import os
import signal
import struct
import sys
import uvloop
uvloop.install()

from concurrent.futures import ThreadPoolExecutor
from pysolarmanv5 import PySolarmanV5Async, V5FrameError


log = logging.getLogger('Exporter')


class Solarman:
    def __init__(self, loop):
        # Setup logging
        self._setup_logging()
        # Load environment variables
        self._load_env_vars()

        # Get the event loop
        self.loop: asyncio.BaseEventLoop = loop

        self.targets: list[dict[str, int | str | bool]] = []

        # Queue of data waiting to be inserted into ClickHouse
        self.clickhouse_queue: asyncio.Queue = asyncio.Queue(maxsize=self.clickhouse_queue_limit)

        self.last_reading: dict[int, int | float] = {}

        # Event used to stop the loop
        self.stop_event: asyncio.Event = asyncio.Event()

    def _setup_logging(self):
        """
            Sets up logging colors and formatting
        """
        # Create a new handler with colors and formatting
        shandler = logging.StreamHandler(stream=sys.stdout)
        shandler.setFormatter(colorlog.LevelFormatter(
            fmt={
                'DEBUG': '{log_color}{asctime} [{levelname}] {message}',
                'INFO': '{log_color}{asctime} [{levelname}] {message}',
                'WARNING': '{log_color}{asctime} [{levelname}] {message}',
                'ERROR': '{log_color}{asctime} [{levelname}] {message}',
                'CRITICAL': '{log_color}{asctime} [{levelname}] {message}',
            },
            log_colors={
                'DEBUG': 'blue',
                'INFO': 'white',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bg_red',
            },
            style='{',
            datefmt='%H:%M:%S'
        ))
        # Add the new handler
        logging.getLogger('Exporter').addHandler(shandler)
        logging.getLogger('pysolarmanv5').addHandler(shandler)
        log.debug('Finished setting up logging')

    def _load_targets(self):
        # Open and read the targets file
        with open('targets.json', 'r') as file:
            try:
                targets = json.loads(file.read())
            except Exception as e:
                log.error(f'Failed to read targets.json: "{e}"')
                exit(1)

        # Parse targets
        for target in targets:
            try:
                if (port := target.get('port', 161)):
                    try:
                        port = int(port)
                    except ValueError:
                        log.error(f'Invalid port "{port}" for target "{target["name"]}"')
                        continue

                if (serial_number := target.get('serial_number')) is None:
                    log.error(f'Missing required key "serial_number" for target "{target["name"]}"')
                    continue
                else:
                    try:
                        serial_number = int(serial_number)
                    except ValueError:
                        log.error(f'Invalid serial_number "{serial_number}" for target "{target["name"]}"')
                        continue

                if (mb_slave_id := target.get('mb_slave_id', 1)):
                    try:
                        mb_slave_id = int(mb_slave_id)
                    except ValueError:
                        log.error(f'Invalid mb_slave_id "{mb_slave_id}" for target "{target["name"]}"')
                        continue

                if (interval := target.get('interval', self.fetch_interval)):
                    try:
                        interval = int(interval)
                    except ValueError:
                        log.error(f'Invalid interval "{interval}" for target "{target["name"]}"')
                        continue

                if (timeout := target.get('timeout', self.fetch_timeout)):
                    try:
                        timeout = int(timeout)
                    except ValueError:
                        log.error(f'Invalid timeout "{timeout}" for target "{target["name"]}"')
                        continue

                if (error_correction := target.get('error_correction', False)):
                    try:
                        error_correction = bool(error_correction)
                    except ValueError:
                        log.error(f'Invalid error_correction "{error_correction}" for target "{target["name"]}"')
                        continue

                self.targets.append({
                    'name': target['name'], # Inverter name
                    'ip': target['ip'], # Logging stick IP
                    'port': port, # Logging stick Modbus port
                    'serial_number': serial_number, # Logging stick serial number
                    'mb_slave_id': mb_slave_id, # Modbus slave ID
                    'interval': interval, # Modbus Fetch interval
                    'timeout': timeout, # Modbus fetch timeout
                    'error_correction': error_correction, # Solarman V5 error correction
                })
                log.debug(f'Parsed target "{target["name"]}" at IP "{target["ip"]}"')
            except KeyError as e:
                log.error(f'Missing required key "{e.args[0]}" for target "{target["name"]}"')
            except Exception:
                log.exception(f'Failed to parse target {target}')

    def _load_env_vars(self):
        """
            Loads environment variables
        """
        # Max number of inserts waiting to be inserted at once
        try:
            self.clickhouse_queue_limit = int(os.environ.get('CLICKHOUSE_QUEUE_LIMIT', 50))
        except ValueError:
            log.exception('Invalid CLICKHOUSE_QUEUE_LIMIT passed, must be a number')
            exit(1)

        # Default global SNMP fetch interval
        try:
            self.fetch_interval = int(os.environ.get('FETCH_INTERVAL', 30))
        except ValueError:
            log.exception('Invalid FETCH_INTERVAL passed, must be a number')
            exit(1)

        # Default global Modbus fetch timeout
        try:
            self.fetch_timeout = int(os.environ.get('FETCH_TIMEOUT', 15))
        except ValueError:
            log.exception('Invalid FETCH_TIMEOUT passed, must be a number')
            exit(1)

        # Log level to use
        # 10/debug  20/info  30/warning  40/error
        try:
            self.log_level = int(os.environ.get('LOG_LEVEL', 20))
        except ValueError:
            log.exception('Invalid LOG_LEVEL passed, must be a number')
            exit(1)

        # Set the logging level
        logging.getLogger('Exporter').setLevel(self.log_level)
        # Set the pysolarmanv5 logging level
        logging.getLogger('pysolarmanv5').setLevel(self.log_level)

        # ClickHouse info
        try:
            self.clickhouse_url = os.environ['CLICKHOUSE_URL']
            self.clickhouse_user = os.environ['CLICKHOUSE_USER']
            self.clickhouse_pass = os.environ['CLICKHOUSE_PASS']
            self.clickhouse_db = os.environ['CLICKHOUSE_DB']
        except KeyError as e:
            log.error(f'Missing required environment variable "{e.args[0]}"')
            exit(1)
        self.clickhouse_table = os.environ.get('CLICKHOUSE_TABLE', 'solis_solarman')

    async def insert_to_clickhouse(self):
        """
            Gets data from the data queue and inserts it into ClickHouse
        """
        while True:
            # Get and check data from the queue
            if not (data := await self.clickhouse_queue.get()):
                continue

            # Keep trying until the insert succeeds
            while True:
                try:
                    # Insert the data into ClickHouse
                    log.debug(f'Got data to insert: {data}')
                    await self.clickhouse.execute(

                        f"""
                        INSERT INTO {self.clickhouse_table} (
                            inverter, inverter_temperature_celsius, inverter_efficiency_percent, dc_1_voltage, dc_1_amps,
                            dc_1_watts, dc_2_voltage, dc_2_amps, dc_2_watts, dc_3_voltage, dc_3_amps,
                            dc_3_watts, dc_calculated_watts, dc_actual_watts, dc_busbar_voltage,
                            ground_voltage, ac_apparent_watts, ac_actual_watts, ac_voltage,
                            ac_amps, ac_frequency, kwh_day, kwh_month, kwh_annual, kwh_total, time
                        ) VALUES
                        """,
                        data
                    )
                    log.debug(f'Inserted data for timestamp {data[-1]}')
                    # Insert succeeded, break the loop and move on
                    break
                except Exception as e:
                    log.error(f'Insert failed for timestamp {data[-1]}: "{e}"')
                    # Wait before retrying so we don't spam retries
                    await asyncio.sleep(2)

    async def fetch_inverter(self, inverter:dict[str, int | str | bool]) -> None:
        log.info(f'Starting fetch for target "{inverter["name"]}" at IP "{inverter["ip"]}"')
        while True:
            try:
                modbus = PySolarmanV5Async(
                    address=inverter['ip'],
                    serial=inverter['serial_number'],
                    port=inverter['port'],
                    mb_slave_id=inverter['mb_slave_id'],
                    socket_timeout=inverter['timeout'],
                    v5_error_correction=inverter['error_correction'],
                    auto_reconnect=True,
                    logger=logging.getLogger('pysolarmanv5')
                )
                # Connect the modbus and start the reader loop
                await modbus.connect()
                log.debug(f'Created initial modbus connection to target "{inverter["name"]}" at IP "{inverter["ip"]}"')
                break
            except Exception as e:
                # The modbus will fail to create if the logging stick is offline
                log.debug(f'Failed to create initial modbus for target "{inverter["name"]}": "{e}"')
                # Retry until it comes back online, but don't spam retry
                await asyncio.sleep(15)

        while True:
            try:
                log.debug(f'Fetching data for target "{inverter["name"]}" at IP "{inverter["ip"]}"')
                # DC 1 Voltage (0.1 V)
                dc_1_voltage = await self.read_input_register(modbus, register_addr=3021, quantity=1, scale=0.1)
                # DC 1 Current (0.1 A)
                dc_1_amps = await self.read_input_register(modbus, register_addr=3022, quantity=1, scale=0.1)
                # DC 1 Calculated Power (1 W)
                dc_1_watts = dc_1_voltage * dc_1_amps

                # DC 2 Voltage (0.1 V)
                dc_2_voltage = await self.read_input_register(modbus, register_addr=3023, quantity=1, scale=0.1)
                # DC 2 Current (0.1 A)
                dc_2_amps = await self.read_input_register(modbus, register_addr=3024, quantity=1, scale=0.1)
                # DC 2 Calculated Power (1 W)
                dc_2_watts = dc_2_voltage * dc_2_amps

                # DC 3 Voltage (0.1 V)
                dc_3_voltage = await self.read_input_register(modbus, register_addr=3025, quantity=1, scale=0.1)
                # DC 3 Current (0.1 A)
                dc_3_amps = await self.read_input_register(modbus, register_addr=3026, quantity=1, scale=0.1)
                # DC 3 Calculated Power (1 W)
                dc_3_watts = dc_3_voltage * dc_3_amps                

                # DC Calculated Power (1 W)
                dc_calculated_watts = dc_1_watts + dc_2_watts + dc_3_watts
                # DC Actual Power (1 W)
                dc_actual_watts =	await self.read_input_register(modbus, register_addr=3006, quantity=2)
                # AC Actual Power (1 W)
                ac_actual_watts = await self.read_input_register(modbus, register_addr=3004, quantity=2)

                data = [
                    inverter['name'], # Inverter name
                    await self.read_input_register(modbus, register_addr=3041, quantity=1, scale=0.1), # Inverter temperature
                    min(100.0, ac_actual_watts / dc_actual_watts * 100), # Inverter DC > AC efficiency
                    dc_1_voltage, # DC1 Voltage
                    dc_1_amps, # DC1 Amps
                    dc_1_watts, # DC1 Calculated Power
                    dc_2_voltage, # DC2 Voltage
                    dc_2_amps, # DC2 Amps
                    dc_2_watts, # DC2 Calculated Power
                    dc_3_voltage, # DC3 Voltage
                    dc_3_amps, # DC3 Amps
                    dc_3_watts, # DC3 Calculated Power
                    dc_calculated_watts, # DC Calculated Power
                    dc_actual_watts, # DC Actual Power
                    await self.read_input_register(modbus, register_addr=3031, quantity=1, scale=0.1), # DC Busbar Voltage (0.1 V)
                    await self.read_input_register(modbus, register_addr=3030, quantity=1, scale=0.1), # Ground Voltage (0.1 V)
                    await self.read_input_register(modbus, register_addr=3057, quantity=2), # AC Apparent Power (1 VA)
                    ac_actual_watts, # AC Actual Power (1 W)
                    await self.read_input_register(modbus, register_addr=3035, quantity=1, scale=0.1), # AC Voltage (0.1 V)
                    await self.read_input_register(modbus, register_addr=3038, quantity=1, scale=0.1), # AC Amps (0.1 A)
                    await self.read_input_register(modbus, register_addr=3042, quantity=1, scale=0.01), # AC Frequency (0.01 Hz)
                    await self.read_input_register(modbus, register_addr=3014, quantity=1, scale=0.1), # Daily yield (kWh)
                    await self.read_input_register(modbus, register_addr=3010, quantity=2), # Monthly yield (kWh)
                    await self.read_input_register(modbus, register_addr=3016, quantity=2), # Annual yield (kWh)
                    await self.read_input_register(modbus, register_addr=3008, quantity=2), # Total yield (kWh)
                    datetime.datetime.now(tz=datetime.timezone.utc).timestamp() # Current UTC timestamp
                ]

                # Insert the data into the ClickHouse queue
                self.clickhouse_queue.put_nowait(data)

            except Exception as e:
                log.exception(f'Failed to fetch target "{inverter["name"]}" at IP "{inverter["ip"]}": {e}')

            finally:
                # Wait the configured interval before fetching again
                await asyncio.sleep(inverter['interval'])

    # Returns either a float or None
    async def read_input_register(self, modbus:PySolarmanV5Async, register_addr:int, quantity:int, scale:float=1.0) -> float:
        # Some modbus reads fail, so we need to retry them
        while True:
            try:
                fut = modbus.read_input_register_formatted(register_addr=register_addr, quantity=quantity, scale=scale)
                # It gets stuck waiting for a response sometimes even with socket_timeout
                # so we need a manual async timeout here
                result = await asyncio.wait_for(fut, timeout=modbus.socket_timeout)
            except (TimeoutError, V5FrameError, struct.error, umodbus.exceptions.ModbusError) as e:
                log.error(f'Failed to read register {register_addr}: {e}')
                # It's most likely gonna return invalid values now, so sleep for a bit
                await asyncio.sleep(1)
                continue

            self.last_reading[register_addr] = result

            return result

    async def run(self):
        """
            Setup and run the exporter
        """
        # Load the targets from targets.json
        self._load_targets()
        if not self.targets:
            log.error('No valid targets found in targets.json')
            exit(1)

        # Create a ClientSession that doesn't verify SSL certificates
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False)
        )
        # Create the ClickHouse client
        self.clickhouse = aiochclient.ChClient(
            self.session,
            url=self.clickhouse_url,
            user=self.clickhouse_user,
            password=self.clickhouse_pass,
            database=self.clickhouse_db,
            json=json
        )
        log.debug(f'Using ClickHouse table "{self.clickhouse_table}" at "{self.clickhouse_url}"')

        # Run the queue inserter as a task
        asyncio.create_task(self.insert_to_clickhouse())

        for inverter in self.targets:
            # Run the fetcher as a task
            log.debug(f'Creating task for target {inverter}')
            asyncio.create_task(self.fetch_inverter(inverter))

        # Run forever or until we get SIGTERM'd
        await self.stop_event.wait()

        log.info('Exiting...')
        # Close the ClientSession
        await self.session.close()
        # Close the ClickHouse client
        await self.clickhouse.close()


loop = asyncio.new_event_loop()
# Create a new thread pool and set it as the default executor
loop.set_default_executor(ThreadPoolExecutor())
# Create an instance of the exporter
solarman = Solarman(loop)

def sigterm_handler(_signo, _stack_frame):
    """
        Handle SIGTERM
    """
    # Set the event to stop the exporter loop
    solarman.stop_event.set()

# Register the SIGTERM handler
signal.signal(signal.SIGTERM, sigterm_handler)
# Run the loop
loop.run_until_complete(solarman.run())
