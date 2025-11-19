"""
logger.py
=========

Logging configuration for Shortest Path Computation project.

This module sets up logging to both terminal and file simultaneously.
All log messages are written to both stdout and a log file.

Usage:
------
    from logger import get_logger
    
    logger = get_logger(__name__)
    logger.info("Processing started")
    logger.warning("Low memory")
    logger.error("Critical error occurred")

Output:
-------
    - Terminal (stdout): Real-time monitoring
    - File (logs/): Persistent logging for analysis

Author: [Your Name]
Date: 2025
"""

import logging
import os
from datetime import datetime
from pathlib import Path


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

# Create logs directory if it doesn't exist
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Generate log filename with timestamp
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"shortest_path_{TIMESTAMP}.log"

# Log levels
LOG_LEVEL = logging.INFO

# Log format: [LEVEL] timestamp - logger_name - message
LOG_FORMAT = logging.Formatter(
    fmt='[%(levelname)-8s] %(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


# ============================================================================
# LOGGING SETUP FUNCTION
# ============================================================================

def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger that writes to both terminal and file.
    
    This function creates a logger that outputs to:
    1. Terminal (console) - For real-time monitoring
    2. Log file - For persistent record keeping
    
    Args:
        name: Logger name (usually __name__ from calling module)
    
    Returns:
        Configured logging.Logger instance
    
    Example:
        >>> from logger import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("Starting computation")
        [INFO    ] 2025-01-15 10:30:45 - __main__ - Starting computation
    """
    
    logger = logging.getLogger(name)
    
    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(LOG_LEVEL)
        
        # ===== Console Handler (Terminal Output) =====
        console_handler = logging.StreamHandler()
        console_handler.setLevel(LOG_LEVEL)
        console_handler.setFormatter(LOG_FORMAT)
        logger.addHandler(console_handler)
        
        # ===== File Handler (Log File Output) =====
        file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
        file_handler.setLevel(LOG_LEVEL)
        file_handler.setFormatter(LOG_FORMAT)
        logger.addHandler(file_handler)
    
    return logger


# ============================================================================
# LOGGING UTILITIES
# ============================================================================

def log_section(logger: logging.Logger, title: str, width: int = 60):
    """
    Log a section header with decorative separators.
    
    Creates a formatted section header for better log readability.
    
    Args:
        logger: Logger instance
        title: Section title
        width: Width of separator line
    
    Example:
        >>> log_section(logger, "Processing Resolution 14")
        ============================================================
        Processing Resolution 14
        ============================================================
    """
    separator = "=" * width
    logger.info(separator)
    logger.info(title)
    logger.info(separator)


def log_table(logger: logging.Logger, headers: list, rows: list):
    """
    Log data in table format.
    
    Args:
        logger: Logger instance
        headers: List of column headers
        rows: List of rows (each row is a list of values)
    
    Example:
        >>> log_table(logger, 
        ...           ["Resolution", "Count", "Time"],
        ...           [["14", "97963", "5.2s"],
        ...            ["13", "45000", "3.1s"]])
    """
    # Calculate column widths
    col_widths = [max(len(str(h)), 
                      max(len(str(row[i])) for row in rows)) 
                  for i, h in enumerate(headers)]
    
    # Format header
    header = " | ".join(h.ljust(w) for h, w in zip(headers, col_widths))
    separator = "-" * len(header)
    
    logger.info(separator)
    logger.info(header)
    logger.info(separator)
    
    # Format rows
    for row in rows:
        row_str = " | ".join(str(v).ljust(w) for v, w in zip(row, col_widths))
        logger.info(row_str)
    
    logger.info(separator)


def log_dict(logger: logging.Logger, data: dict, title: str = None):
    """
    Log dictionary in formatted key-value pairs.
    
    Args:
        logger: Logger instance
        data: Dictionary to log
        title: Optional title for the section
    
    Example:
        >>> config = {"edges": "data.csv", "resolution": 14}
        >>> log_dict(logger, config, "Configuration")
    """
    if title:
        logger.info(f"--- {title} ---")
    
    max_key_len = max(len(str(k)) for k in data.keys())
    for key, value in data.items():
        logger.info(f"{str(key).ljust(max_key_len)} : {value}")


# ============================================================================
# LOG FILE INFORMATION
# ============================================================================

def print_log_info():
    """
    Print information about current log file to console.
    
    Useful for notifying user where logs are being saved.
    """
    logger = get_logger(__name__)
    logger.info(f"Log file: {LOG_FILE}")
    logger.info(f"Log directory: {LOG_DIR.absolute()}")


# ============================================================================
# MAIN - Test logging
# ============================================================================

if __name__ == "__main__":
    # Create test logger
    test_logger = get_logger("test")
    
    # Test different log levels
    print("\n" + "="*60)
    print("Testing Logger Output")
    print("="*60 + "\n")
    
    test_logger.debug("Debug message (not shown - level is INFO)")
    test_logger.info("Info message")
    test_logger.warning("Warning message")
    test_logger.error("Error message")
    
    # Test section logging
    log_section(test_logger, "Testing Section Header")
    
    # Test table logging
    log_table(test_logger, 
              ["Resolution", "Count", "Time"],
              [["14", "97963", "5.2s"],
               ["13", "45000", "3.1s"],
               ["12", "12000", "1.5s"]])
    
    # Test dict logging
    log_dict(test_logger, 
             {"edges_file": "data/edges.csv",
              "graph_file": "data/graph.csv",
              "resolution_range": "14-8"},
             "Configuration")
    
    # Print log file location
    print_log_info()
    
    print(f"\n✓ Log file saved to: {LOG_FILE}")
    print(f"✓ Check '{LOG_DIR}' folder for all log files\n")