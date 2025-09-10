#!/usr/bin/env python3
"""
Timezone management utilities for Kraken Analyzer.

Provides functions to handle timezone conversions between UTC and CET,
standardize timestamp parsing, and format timestamps for different use cases.
"""

from datetime import datetime, timezone, timedelta
from typing import Union, Optional
import logging

logger = logging.getLogger(__name__)

# Define timezone objects
UTC = timezone.utc
CET = timezone(timedelta(hours=1))  # Central European Time (UTC+1)
CEST = timezone(timedelta(hours=2))  # Central European Summer Time (UTC+2)


def parse_timestamp_flexible(timestamp_str: str, assume_timezone: Optional[timezone] = None) -> datetime:
    """
    Parse timestamp string with flexible format handling.
    
    Args:
        timestamp_str: Timestamp string in various formats
        assume_timezone: Timezone to assume if none specified
        
    Returns:
        Timezone-aware datetime object
    """
    # Handle format like "2025:09:10T06:18:09Z"  
    if ":" in timestamp_str and timestamp_str.count(":") > 2:
        # Convert "2025:09:10T06:18:09Z" to "2025-09-10T06:18:09Z"
        timestamp_str = timestamp_str.replace(":", "-", 2)
    
    # Handle 'Z' suffix (UTC)
    if timestamp_str.endswith('Z'):
        timestamp_str = timestamp_str.rstrip('Z')
        dt = datetime.fromisoformat(timestamp_str)
        return dt.replace(tzinfo=UTC)
    
    # Try to parse as ISO format
    dt = datetime.fromisoformat(timestamp_str)
    
    # If no timezone info, assume the provided timezone or UTC
    if dt.tzinfo is None:
        assume_tz = assume_timezone or UTC
        dt = dt.replace(tzinfo=assume_tz)
        logger.debug(f"Assumed timezone {assume_tz} for timestamp {timestamp_str}")
    
    return dt


def convert_utc_to_cet(utc_dt: datetime) -> datetime:
    """
    Convert UTC datetime to CET.
    
    Args:
        utc_dt: UTC datetime object
        
    Returns:
        CET datetime object
    """
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=UTC)
    elif utc_dt.tzinfo != UTC:
        utc_dt = utc_dt.astimezone(UTC)
    
    return utc_dt.astimezone(CET)


def convert_cet_to_utc(cet_dt: Union[datetime, str]) -> datetime:
    """
    Convert CET datetime to UTC.
    
    Args:
        cet_dt: CET datetime object or string
        
    Returns:
        UTC datetime object
    """
    if isinstance(cet_dt, str):
        cet_dt = datetime.fromisoformat(cet_dt)
    
    if cet_dt.tzinfo is None:
        cet_dt = cet_dt.replace(tzinfo=CET)
    elif cet_dt.tzinfo != CET:
        # If not CET, convert from current timezone to CET first
        logger.warning(f"Converting from {cet_dt.tzinfo} to CET before UTC conversion")
        cet_dt = cet_dt.astimezone(CET)
    
    return cet_dt.astimezone(UTC)


def format_timestamp_for_filename(dt: datetime) -> str:
    """
    Format datetime for use in filenames (like run_results.TIMESTAMP.csv).
    
    Args:
        dt: Datetime object
        
    Returns:
        Filename-safe timestamp string
    """
    # Convert to UTC if needed
    if dt.tzinfo != UTC:
        dt = dt.astimezone(UTC)
    
    # Format: 2025-09-10T06-18-09Z
    return dt.strftime("%Y-%m-%dT%H-%M-%SZ")


def format_timestamp_iso(dt: datetime) -> str:
    """
    Format datetime as ISO string.
    
    Args:
        dt: Datetime object
        
    Returns:
        ISO format timestamp string
    """
    return dt.isoformat()


def get_current_utc() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(UTC)


def get_current_cet() -> datetime:
    """Get current CET datetime."""
    return datetime.now(CET)


def is_same_day_cet(dt1: datetime, dt2: datetime) -> bool:
    """
    Check if two datetime objects represent the same day in CET.
    
    Args:
        dt1: First datetime
        dt2: Second datetime
        
    Returns:
        True if same day in CET
    """
    cet1 = convert_utc_to_cet(dt1) if dt1.tzinfo == UTC else dt1.astimezone(CET)
    cet2 = convert_utc_to_cet(dt2) if dt2.tzinfo == UTC else dt2.astimezone(CET)
    
    return cet1.date() == cet2.date()


def timestamp_range_summary(timestamps: list[str]) -> dict:
    """
    Provide summary statistics for a list of timestamp strings.
    
    Args:
        timestamps: List of timestamp strings
        
    Returns:
        Dictionary with min, max, count, and timezone info
    """
    if not timestamps:
        return {"count": 0, "min": None, "max": None}
    
    parsed_timestamps = []
    for ts in timestamps:
        try:
            parsed_timestamps.append(parse_timestamp_flexible(ts))
        except Exception as e:
            logger.warning(f"Could not parse timestamp {ts}: {e}")
    
    if not parsed_timestamps:
        return {"count": 0, "min": None, "max": None}
    
    return {
        "count": len(parsed_timestamps),
        "min": min(parsed_timestamps),
        "max": max(parsed_timestamps),
        "min_cet": convert_utc_to_cet(min(parsed_timestamps)),
        "max_cet": convert_utc_to_cet(max(parsed_timestamps)),
        "span_days": (max(parsed_timestamps) - min(parsed_timestamps)).days
    }


def main():
    """CLI interface for timezone utilities."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Timezone utilities for Kraken Analyzer")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Convert command
    convert_parser = subparsers.add_parser('convert', help='Convert timestamps between timezones')
    convert_parser.add_argument('timestamp', help='Timestamp string to convert')
    convert_parser.add_argument('--from-tz', choices=['UTC', 'CET'], default='UTC', help='Source timezone')
    convert_parser.add_argument('--to-tz', choices=['UTC', 'CET'], default='CET', help='Target timezone')
    
    # Current time command
    now_parser = subparsers.add_parser('now', help='Show current time in different timezones')
    
    # Parse timestamp command
    parse_parser = subparsers.add_parser('parse', help='Parse and analyze timestamp')
    parse_parser.add_argument('timestamp', help='Timestamp string to parse')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    
    if args.command == 'convert':
        dt = parse_timestamp_flexible(args.timestamp)
        
        if args.from_tz == 'UTC' and args.to_tz == 'CET':
            result = convert_utc_to_cet(dt)
        elif args.from_tz == 'CET' and args.to_tz == 'UTC':
            result = convert_cet_to_utc(dt)
        else:
            result = dt  # No conversion needed
        
        print(f"Input: {args.timestamp}")
        print(f"Parsed: {dt}")
        print(f"Converted ({args.from_tz} -> {args.to_tz}): {result}")
        print(f"ISO format: {format_timestamp_iso(result)}")
        print(f"Filename format: {format_timestamp_for_filename(result)}")
    
    elif args.command == 'now':
        utc_now = get_current_utc()
        cet_now = get_current_cet()
        
        print(f"UTC: {utc_now}")
        print(f"CET: {cet_now}")
        print(f"UTC ISO: {format_timestamp_iso(utc_now)}")
        print(f"CET ISO: {format_timestamp_iso(cet_now)}")
        print(f"UTC filename: {format_timestamp_for_filename(utc_now)}")
    
    elif args.command == 'parse':
        dt = parse_timestamp_flexible(args.timestamp)
        dt_cet = convert_utc_to_cet(dt) if dt.tzinfo == UTC else dt.astimezone(CET)
        
        print(f"Input: {args.timestamp}")
        print(f"Parsed: {dt}")
        print(f"Timezone: {dt.tzinfo}")
        print(f"UTC: {dt.astimezone(UTC) if dt.tzinfo != UTC else dt}")
        print(f"CET: {dt_cet}")
        print(f"ISO format: {format_timestamp_iso(dt)}")
        print(f"Filename format: {format_timestamp_for_filename(dt)}")
    
    return 0


if __name__ == "__main__":
    exit(main())