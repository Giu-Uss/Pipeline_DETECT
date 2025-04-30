from psycopg2 import sql

# ==============================================================================
# Pipeline_upload_to_mapvisualization.py
# ==============================================================================
# Author: MSc. Giuseppe Ussia
# Supervisor: Prof. Luciana Fenoglio
# 
#
# Description:
# ------------
# This script handles the ingestion of DETECT-compliant NetCDF (.nc) time series 
# data into a PostgreSQL/PostGIS database used by the MapVisualisation platform.
#
# It performs:
#   1. Insertion of static geographic metadata into the coordinate table.
#   2. Ingestion of time series measurements into the value table.
#   3. Construction of spatial geometry using PostGIS.
#
# The structure is configurable and supports reuse across different datasets 
# with minor updates to the configuration block.
#
# Usage:
# ------
# - Set the configuration fields (NetCDF path, table names, etc.)
# - Run with: python upload_to_mapvisualization.py
# ==============================================================================

import psycopg2
import h5py
import numpy as np
from datetime import datetime, timedelta

# -----------------------------
# CONFIGURATION SECTION
# -----------------------------
DB_PARAMS = {
    'host': '131.220.186.82',
    'database': 'mapvisualisation',
    'user': 'gussia',
    'password': ***
}

DATASET_CONFIG = {
    'file_path': '/path/to/your/file.nc',              # <--- Update this
    'coordinate_table': 'your_coordinate_table',       # <--- Update this
    'value_table': 'your_value_table',                 # <--- Update this
    'geometry_epsg': 4326

    'value_column': 'value__your_parameter_name',  # <-- Update this}

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def mjd2date(mjd_array):
    """Convert Modified Julian Date (MJD) to datetime."""
    mjd_origin = datetime(1858, 11, 17)
    return [mjd_origin + timedelta(days=float(m)) for m in mjd_array]

# -----------------------------
# CORE FUNCTION
# -----------------------------
def extract_and_insert(file_path, coordinate_table, value_table):
    try:
        with h5py.File(file_path, 'r') as f:
            print(f" Processing file: {file_path}")

            info = f.get('Info')
            if info is None:
                print(" 'Info' group not found.")
                return

            # Determine IDs and coordinate fields
            ids = info['node_id'][:] if 'node_id' in info else info['reach_id'][:]
            names = [n.decode('utf-8') if isinstance(n, bytes) else n for n in info['river_name'][:]] if 'river_name' in info else [None]*len(ids)
            xs = info['x'][:] if 'x' in info else info['x_reach'][:]
            ys = info['y'][:] if 'y' in info else info['y_reach'][:]

            print(" Inserting coordinates...")
            for id_, name, x, y in zip(ids, names, xs, ys):
                try:
                    cursor.execute(f'''
                        INSERT INTO {coordinate_table} (id, name, lat, lon, geonetwork_data_catalog, geom)
                        VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), {DATASET_CONFIG['geometry_epsg']}))
                        ON CONFLICT (id) DO NOTHING;
                    ''', (int(id_), name, float(y), float(x), None, float(x), float(y)))
                except Exception as e:
                    print(f" Error inserting coordinate id {id_}: {e}")

            # Identify main data group
            db_group_name = 'LakeDB' if 'LakeDB' in f else 'SWOTDB' if 'SWOTDB' in f else 'AltDB'
            db_group = f.get(db_group_name)
            if db_group is None:
                print(" Data group not found.")
                return

            print(" Inserting time series...")
            count = 0
            for subgroup_name in db_group:
                subgroup = db_group[subgroup_name]
                id_ = int(subgroup_name)
                times = mjd2date(subgroup['time'][:])
                values = subgroup[list(subgroup.keys())[1]][:]

                for t, v in zip(times, values):
                    try:
                        cursor.execute(f'''
                            INSERT INTO {value_table} (id, time, value)
                            VALUES (%s, %s, %s);
                        ''', (id_, t, float(v)))
                        count += 1
                    except Exception as e:
                        print(f" Error inserting value for id {id_}: {e}")

            conn.commit()
            print(f" Successfully inserted {count} records.")

    except Exception as e:
        print(" File processing failed:", e)

# -----------------------------
# MAIN EXECUTION
# -----------------------------
if __name__ == '__main__':
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        print(" Connected to the database.")
        extract_and_insert(
            DATASET_CONFIG['file_path'],
            DATASET_CONFIG['coordinate_table'],
            DATASET_CONFIG['value_table']
        )
    finally:
        cursor.close()
        conn.close()
        print(" Connection closed.")
