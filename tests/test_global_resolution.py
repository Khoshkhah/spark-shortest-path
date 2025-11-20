import sys
from pathlib import Path
import logging

# Add src directory to path
project_root = Path.cwd()
sys.path.insert(0, str(project_root / 'src'))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utilities import initialize_spark, add_parent_cell_at_resolution
import h3

def test_global_resolution_handling():
    print("Testing global resolution handling...")
    
    spark = initialize_spark(app_name="TestGlobalResolution")
    
    # Generate a valid H3 cell from coordinates
    lat, lng = 37.7749, -122.4194 # San Francisco
    cell = h3.latlng_to_cell(lat, lng, 15)
    
    data = [
        (cell,), 
    ]
    df = spark.createDataFrame(data, ["via_cell"])
    
    # Test resolution -1
    print("Testing resolution -1...")
    df_global = add_parent_cell_at_resolution(df, -1)
    
    # Check if 'current_cell' is 'global'
    rows = df_global.collect()
    for row in rows:
        assert row.current_cell == "global", f"Expected 'global', got {row.current_cell}"
    
    print("✓ Resolution -1 correctly returns 'global' partition key")
    
    # Test resolution 0
    print("Testing resolution 0...")
    df_res0 = add_parent_cell_at_resolution(df, 0)
    rows = df_res0.collect()
    for row in rows:
        # Just check it's a valid H3 index
        assert row.current_cell is not None
        assert h3.is_valid_cell(row.current_cell)
        assert h3.get_resolution(row.current_cell) == 0
        
    print("✓ Resolution 0 correctly returns valid H3 cell")
    
    spark.stop()

if __name__ == "__main__":
    test_global_resolution_handling()
