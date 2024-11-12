import pandas as pd
import sqlalchemy
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.parquet as pq

def create_db_connection():
    # Get database connection details from environment variables
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    
    connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return sqlalchemy.create_engine(connection_string)

def transform_acu_chunk(df):
    """Transform a chunk of ACU data into the signals format"""
    # Ensure created_at maintains microsecond precision
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    signals = []
    
    # Define the columns to transform
    numeric_columns = {
        'accumulator_voltage': 'acu_accumulator_voltage',
        'accumulator_current': 'acu_accumulator_current',
        'max_cell_temp': 'acu_max_cell_temp',
        'ts_voltage': 'acu_ts_voltage',
        'max_bal_resistor_temp': 'acu_max_bal_resistor_temp',
        'sdc_voltage': 'acu_sdc_voltage',
        'glv_voltage': 'acu_glv_voltage',
        'state_of_charge': 'acu_state_of_charge',
        'fan1_speed': 'acu_fan1_speed',
        'fan2_speed': 'acu_fan2_speed',
        'fan3_speed': 'acu_fan3_speed',
        'pump_speed': 'acu_pump_speed',
        'acu_temp1': 'acu_temp1',
        'acu_temp2': 'acu_temp2',
        'acu_temp3': 'acu_temp3'
    }
    
    boolean_columns = {
        'over_temp_error': 'acu_over_temp_error',
        'over_voltage_error': 'acu_over_voltage_error',
        'over_current_error': 'acu_over_current_error',
        'bms_error': 'acu_bms_error',
        'under_voltage_error': 'acu_under_voltage_error',
        'precharge_error': 'acu_precharge_error',
        'teensy_error': 'acu_teensy_error',
        'under_temp_error': 'acu_under_temp_error',
        'is_air_positive': 'acu_air_positive',
        'is_air_negative': 'acu_air_negative',
        'is_precharging': 'acu_precharging',
        'is_precharge_done': 'acu_precharge_done',
        'is_shutdown': 'acu_shutdown'
    }
    
    # Add cell voltage and temperature columns dynamically
    for i in range(128):
        numeric_columns[f'cell{i}_voltage'] = f'acu_cell{i}_voltage'
        numeric_columns[f'cell{i}_temp'] = f'acu_cell{i}_temp'
    
    # Process numeric columns
    for col, signal_id in numeric_columns.items():
        mask = df[col].notna()
        if mask.any():
            signals.append(pd.DataFrame({
                'signal_id': signal_id,
                'created_at': df.loc[mask, 'created_at'],
                'scaled_value': df.loc[mask, col].astype(float),
                'node': 'acu',
                'millis': df.loc[mask, 'millis']
            }))
    
    # Process boolean columns
    for col, signal_id in boolean_columns.items():
        mask = df[col].notna()
        if mask.any():
            signals.append(pd.DataFrame({
                'signal_id': signal_id,
                'created_at': df.loc[mask, 'created_at'],
                'scaled_value': df.loc[mask, col].astype(float),
                'node': 'acu',
                'millis': df.loc[mask, 'millis']
            }))
    
    if signals:
        return pd.concat(signals, ignore_index=True)
    return None

def process_acu_data():
    """Process ACU data similar to VDM data"""
    engine = create_db_connection()
    
    # Create output directory if it doesn't exist
    output_dir = "signals_output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Define PyArrow schema
    schema = pa.schema([
        ('signal_id', pa.string()),
        ('created_at', pa.timestamp('us')),
        ('scaled_value', pa.float64()),
        ('node', pa.string()),
        ('millis', pa.int64())
    ])
    
    # Process in chunks to manage memory
    chunk_size = 10000
    query = "SELECT * FROM gr24_acu"
    
    # Initialize chunk counter and total rows processed
    chunk_number = 0
    total_rows = 0
    
    for chunk_df in pd.read_sql(query, engine, chunksize=chunk_size):
        transformed_df = transform_acu_chunk(chunk_df)
        
        if transformed_df is not None:
            # Save to parquet file
            output_file = os.path.join(output_dir, f"acu_chunk_{chunk_number}.parquet")
            table = pa.Table.from_pandas(transformed_df, schema=schema)
            pq.write_table(table, output_file, compression='snappy')
            
            total_rows += len(transformed_df)
            print(f"Processed chunk {chunk_number}: {len(transformed_df)} signals generated")
            chunk_number += 1
    
    return total_rows

def transform_vdm_chunk(df):
    """Transform a chunk of VDM data into the signals format"""
    # Ensure created_at maintains microsecond precision
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    signals = []
    
    # Define the columns to transform
    numeric_columns = {
        'mode': 'vdm_mode',
        'state': 'vdm_state',
        'rev_limit': 'vdm_rev_limit',
        'tcm_status': 'vdm_tcm_status',
        'can_status': 'vdm_can_status',
        'system_status': 'vdm_system_status',
        'max_power': 'vdm_max_power',
        'speed': 'vdm_speed',
        'brake_f': 'vdm_brake_f',
        'brake_r': 'vdm_brake_r'
    }
    
    boolean_columns = {
        'is_ams_fault': 'vdm_ams_fault',
        'is_imd_fault': 'vdm_imd_fault',
        'is_bspd_fault': 'vdm_bspd_fault',
        'is_sdc_opened': 'vdm_sdc_opened',
        'motor_temp_warning': 'vdm_motor_temp_warning',
        'motor_temp_limit': 'vdm_motor_temp_limit'
    }
    
    # Process numeric columns
    for col, signal_id in numeric_columns.items():
        mask = df[col].notna()
        if mask.any():
            signals.append(pd.DataFrame({
                'signal_id': signal_id,
                'created_at': df.loc[mask, 'created_at'],
                'scaled_value': df.loc[mask, col].astype(float),
                'node': 'vdm',
                'millis': df.loc[mask, 'millis']
            }))
    
    # Process boolean columns
    for col, signal_id in boolean_columns.items():
        mask = df[col].notna()
        if mask.any():
            signals.append(pd.DataFrame({
                'signal_id': signal_id,
                'created_at': df.loc[mask, 'created_at'],
                'scaled_value': df.loc[mask, col].astype(float),
                'node': 'vdm',
                'millis': df.loc[mask, 'millis']
            }))
    
    if signals:
        return pd.concat(signals, ignore_index=True)
    return None

def process_vdm_data():
    engine = create_db_connection()
    
    # Create output directory if it doesn't exist
    output_dir = "signals_output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Define PyArrow schema
    schema = pa.schema([
        ('signal_id', pa.string()),
        ('created_at', pa.timestamp('us')),
        ('scaled_value', pa.float64()),
        ('node', pa.string()),
        ('millis', pa.int64())
    ])
    
    # Process in chunks to manage memory
    chunk_size = 10000
    query = "SELECT * FROM gr24_vdm"
    
    # Initialize chunk counter and total rows processed
    chunk_number = 0
    total_rows = 0
    
    for chunk_df in pd.read_sql(query, engine, chunksize=chunk_size):
        transformed_df = transform_vdm_chunk(chunk_df)
        
        if transformed_df is not None:
            # Save to parquet file
            output_file = os.path.join(output_dir, f"vdm_chunk_{chunk_number}.parquet")
            table = pa.Table.from_pandas(transformed_df, schema=schema)
            pq.write_table(table, output_file, compression='snappy')
            
            total_rows += len(transformed_df)
            print(f"Processed chunk {chunk_number}: {len(transformed_df)} signals generated")
            chunk_number += 1
    
    return total_rows

def transform_inverter_chunk(df):
    """Transform a chunk of inverter data into the signals format"""
    # Ensure created_at maintains microsecond precision
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    signals = []
    
    # Define the columns to transform
    numeric_columns = {
        'erpm': 'inverter_erpm',
        'duty_cycle': 'inverter_duty_cycle',
        'input_voltage': 'inverter_input_voltage',
        'current_ac': 'inverter_current_ac',
        'current_dc': 'inverter_current_dc',
        'controller_temp': 'inverter_controller_temp',
        'motor_temp': 'inverter_motor_temp',
        'faults': 'inverter_faults',
        'foc_id': 'inverter_foc_id',
        'fociq': 'inverter_fociq',
        'throttle': 'inverter_throttle',
        'brake': 'inverter_brake',
        'digital_io': 'inverter_digital_io',
        'drive_enable': 'inverter_drive_enable',
        'flags_one': 'inverter_flags_one',
        'flags_two': 'inverter_flags_two',
        'can_version': 'inverter_can_version'
    }
    
    boolean_columns = {
        'overvoltage_error': 'inverter_overvoltage_error',
        'undervoltage_error': 'inverter_undervoltage_error',
        'drv_error': 'inverter_drv_error',
        'overcurrent_error': 'inverter_overcurrent_error',
        'controller_overtemp_error': 'inverter_controller_overtemp_error',
        'motor_overtemp_error': 'inverter_motor_overtemp_error',
        'sensor_wire_error': 'inverter_sensor_wire_error',
        'sensor_general_error': 'inverter_sensor_general_error',
        'can_command_error': 'inverter_can_command_error',
        'analog_input_error': 'inverter_analog_input_error'
    }
    
    # Process numeric columns
    for col, signal_id in numeric_columns.items():
        mask = df[col].notna()
        if mask.any():
            signals.append(pd.DataFrame({
                'signal_id': signal_id,
                'created_at': df.loc[mask, 'created_at'],
                'scaled_value': df.loc[mask, col].astype(float),
                'node': 'inverter',
                'millis': df.loc[mask, 'millis']
            }))
    
    # Process boolean columns
    for col, signal_id in boolean_columns.items():
        mask = df[col].notna()
        if mask.any():
            signals.append(pd.DataFrame({
                'signal_id': signal_id,
                'created_at': df.loc[mask, 'created_at'],
                'scaled_value': df.loc[mask, col].astype(float),
                'node': 'inverter',
                'millis': df.loc[mask, 'millis']
            }))
    
    if signals:
        return pd.concat(signals, ignore_index=True)
    return None

def process_inverter_data():
    """Process inverter data similar to VDM and ACU data"""
    engine = create_db_connection()
    
    # Create output directory if it doesn't exist
    output_dir = "signals_output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Define PyArrow schema
    schema = pa.schema([
        ('signal_id', pa.string()),
        ('created_at', pa.timestamp('us')),
        ('scaled_value', pa.float64()),
        ('node', pa.string()),
        ('millis', pa.int64())
    ])
    
    # Process in chunks to manage memory
    chunk_size = 10000
    query = "SELECT * FROM gr24_inverter"
    
    # Initialize chunk counter and total rows processed
    chunk_number = 0
    total_rows = 0
    
    for chunk_df in pd.read_sql(query, engine, chunksize=chunk_size):
        transformed_df = transform_inverter_chunk(chunk_df)
        
        if transformed_df is not None:
            # Save to parquet file
            output_file = os.path.join(output_dir, f"inverter_chunk_{chunk_number}.parquet")
            table = pa.Table.from_pandas(transformed_df, schema=schema)
            pq.write_table(table, output_file, compression='snappy')
            
            total_rows += len(transformed_df)
            print(f"Processed chunk {chunk_number}: {len(transformed_df)} signals generated")
            chunk_number += 1
    
    return total_rows

def transform_pedal_chunk(df):
    """Transform a chunk of pedal data into the signals format"""
    # Ensure created_at maintains microsecond precision
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    signals = []
    
    # Define the columns to transform
    numeric_columns = {
        'apps_one': 'pedal_apps_one',
        'apps_two': 'pedal_apps_two',
        'apps_one_raw': 'pedal_apps_one_raw',
        'apps_two_raw': 'pedal_apps_two_raw'
    }
    
    # Process numeric columns
    for col, signal_id in numeric_columns.items():
        mask = df[col].notna()
        if mask.any():
            signals.append(pd.DataFrame({
                'signal_id': signal_id,
                'created_at': df.loc[mask, 'created_at'],
                'scaled_value': df.loc[mask, col].astype(float),
                'node': 'pedal',
                'millis': df.loc[mask, 'millis']
            }))
    
    if signals:
        return pd.concat(signals, ignore_index=True)
    return None

def process_pedal_data():
    """Process pedal data similar to other data sources"""
    engine = create_db_connection()
    
    # Create output directory if it doesn't exist
    output_dir = "signals_output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Define PyArrow schema
    schema = pa.schema([
        ('signal_id', pa.string()),
        ('created_at', pa.timestamp('us')),
        ('scaled_value', pa.float64()),
        ('node', pa.string()),
        ('millis', pa.int64())
    ])
    
    # Process in chunks to manage memory
    chunk_size = 10000
    query = "SELECT * FROM gr24_pedal"
    
    # Initialize chunk counter and total rows processed
    chunk_number = 0
    total_rows = 0
    
    for chunk_df in pd.read_sql(query, engine, chunksize=chunk_size):
        transformed_df = transform_pedal_chunk(chunk_df)
        
        if transformed_df is not None:
            # Save to parquet file
            output_file = os.path.join(output_dir, f"pedal_chunk_{chunk_number}.parquet")
            table = pa.Table.from_pandas(transformed_df, schema=schema)
            pq.write_table(table, output_file, compression='snappy')
            
            total_rows += len(transformed_df)
            print(f"Processed chunk {chunk_number}: {len(transformed_df)} signals generated")
            chunk_number += 1
    
    return total_rows

def transform_mobile_chunk(df):
    """Transform a chunk of mobile data into the signals format"""
    # Ensure created_at maintains microsecond precision
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    signals = []
    
    # Define the columns to transform
    numeric_columns = {
        'latitude': 'mobile_latitude',
        'longitude': 'mobile_longitude',
        'altitude': 'mobile_altitude',
        'speed': 'mobile_speed',
        'heading': 'mobile_heading',
        'accelerometer_x': 'mobile_accelerometer_x',
        'accelerometer_y': 'mobile_accelerometer_y',
        'accelerometer_z': 'mobile_accelerometer_z',
        'gyroscope_x': 'mobile_gyroscope_x',
        'gyroscope_y': 'mobile_gyroscope_y',
        'gyroscope_z': 'mobile_gyroscope_z',
        'magnetometer_x': 'mobile_magnetometer_x',
        'magnetometer_y': 'mobile_magnetometer_y',
        'magnetometer_z': 'mobile_magnetometer_z',
        'battery': 'mobile_battery'
    }
    
    # Process numeric columns
    for col, signal_id in numeric_columns.items():
        mask = df[col].notna()
        if mask.any():
            signals.append(pd.DataFrame({
                'signal_id': signal_id,
                'created_at': df.loc[mask, 'created_at'],
                'scaled_value': df.loc[mask, col].astype(float),
                'node': 'mobile',
                'millis': df.loc[mask, 'millis']
            }))
    
    if signals:
        return pd.concat(signals, ignore_index=True)
    return None

def process_mobile_data():
    """Process mobile data similar to other data sources"""
    engine = create_db_connection()
    
    # Create output directory if it doesn't exist
    output_dir = "signals_output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Define PyArrow schema
    schema = pa.schema([
        ('signal_id', pa.string()),
        ('created_at', pa.timestamp('us')),
        ('scaled_value', pa.float64()),
        ('node', pa.string()),
        ('millis', pa.int64())
    ])
    
    # Process in chunks to manage memory
    chunk_size = 10000
    query = "SELECT * FROM gr24_mobile"
    
    # Initialize chunk counter and total rows processed
    chunk_number = 0
    total_rows = 0
    
    for chunk_df in pd.read_sql(query, engine, chunksize=chunk_size):
        transformed_df = transform_mobile_chunk(chunk_df)
        
        if transformed_df is not None:
            # Save to parquet file
            output_file = os.path.join(output_dir, f"mobile_chunk_{chunk_number}.parquet")
            table = pa.Table.from_pandas(transformed_df, schema=schema)
            pq.write_table(table, output_file, compression='snappy')
            
            total_rows += len(transformed_df)
            print(f"Processed chunk {chunk_number}: {len(transformed_df)} signals generated")
            chunk_number += 1
    
    return total_rows

def main():
    start_time = datetime.now()
    print(f"Starting transformation at {start_time}")
    
    try:
        print("Processing VDM data...")
        vdm_rows = process_vdm_data()
        print("Processing ACU data...")
        acu_rows = process_acu_data()
        print("Processing inverter data...")
        inverter_rows = process_inverter_data()
        print("Processing pedal data...")
        pedal_rows = process_pedal_data()
        print("Processing mobile data...")
        mobile_rows = process_mobile_data()
        
        end_time = datetime.now()
        print(f"Transformation completed at {end_time}")
        print(f"Total time: {end_time - start_time}")
        print(f"Total signals generated: VDM={vdm_rows}, ACU={acu_rows}, "
              f"Inverter={inverter_rows}, Pedal={pedal_rows}, Mobile={mobile_rows}, "
              f"Total={vdm_rows + acu_rows + inverter_rows + pedal_rows + mobile_rows}")
    except Exception as e:
        print(f"Error during transformation: {str(e)}")

if __name__ == "__main__":
    main()