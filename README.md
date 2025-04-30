
# Upload Pipeline for MapVisualisation Database 

## Description
This repository provides a reusable Python pipeline for uploading DETECT project time series datasets from NetCDF (.nc) files into the PostgreSQL/PostGIS database used by the **MapVisualisation** platform.

It performs the following operations:

- Parses static metadata (e.g., lake, river, node information) and inserts it into a coordinate table.
- Extracts time series measurements (e.g., water surface height, river discharge) and inserts them into a value table using a configurable column name.
- Creates a standardized SQL view that combines coordinates and measurements for map visualization.
- Grants public read access to relevant tables to enable external visualization tools.

---

## Example Folder Structure
```bash
project_folder/
├── upload_pipeline.py         # Main script (dynamic version)
├── README.md                  # Documentation
├── data/
│   ├── B01-DETECT_SurfaceStorageReservoir_S3FFSAR_20241211.nc
│   ├── B01-DETECT_Node_Discharge_SWOT_20241211.nc
│   └── etc...
```

---

## Requirements
- Python 3.8+
- PostgreSQL 13+ with PostGIS extension
- Required Python libraries:
  - `psycopg2`
  - `h5py`
  - `numpy`

Install them via pip:
```bash
pip install psycopg2 h5py numpy
```

---

## Database SQL Setup
Before running the script, manually create the tables in your PostgreSQL database.

### Coordinate Table (Example)
```sql
CREATE TABLE coordinate__your_dataset (
    id BIGINT PRIMARY KEY,
    name TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    geonetwork_data_catalog TEXT,
    geom geometry(Point,4326)
);
```

### Value Table (Example)
```sql
CREATE TABLE value__your_dataset (
    m_id SERIAL PRIMARY KEY,
    id BIGINT,
    time TIMESTAMP WITHOUT TIME ZONE,
    value__your_parameter_name DOUBLE PRECISION
);
```

### View Creation (Example)
```sql
CREATE OR REPLACE VIEW your_view AS
SELECT
    v.id,
    c.name,
    v.time,
    c.lat,
    c.lon,
    v.value__your_parameter_name AS value,
    c.geonetwork_data_catalog,
    c.geom
FROM
    value__your_dataset v
JOIN
    coordinate__your_dataset c
ON v.id = c.id;

GRANT SELECT ON your_view TO PUBLIC;
```

---

## How to Use the Python Script
1. **Update the `DATASET_CONFIG` dictionary** at the top of the script:
```python
DATASET_CONFIG = {
    'file_path': '/path/to/your/file.nc',
    'coordinate_table': 'coordinate__your_dataset',
    'value_table': 'value__your_dataset',
    'value_column': 'value__your_parameter_name',  # <- dynamic column name
    'geometry_epsg': 4326
}
```

2. **Run the script:**
```bash
python upload_pipeline.py
```

---

## Notes
- Naming conventions follow Farzane Mohseni's guidelines (e.g., `value__parameter_name`).
- The script is now dynamic: you don’t need to modify the code for each dataset—just change the config.
- All uploaded datasets must have a linked GeoNetwork metadata record.
- All views must have public access.

---

## Authors
- **MSc. Giuseppe Ussia** - [gussia@uni-bonn.de]
- **Prof. Luciana Fenoglio** - [fenoglio@uni-bonn.de]

## Collaborators
- **PhD. Farzane Mohseni** - For DETECT database and metadata guidelines.

---

## Acknowledgments
This work is part of the **CRC1502 DETECT** project supported by the German Research Foundation (DFG).

---

## License
Internal use for DETECT CRC1502 members. No public release yet.
