#!/usr/bin/env python3
"""
Experiment period tracking and management utilities.

This module provides functions to track experiment periods, determine correct
experiment labels based on timestamps, and manage experiment transitions.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger(__name__)


class ExperimentPeriodManager:
    """Manages experiment periods and label assignment."""
    
    def __init__(self, periods_file: Path):
        """Initialize with path to experiment periods JSON file."""
        self.periods_file = periods_file
        self._periods: Dict[str, Dict] = {}
        self._load_periods()
    
    def _load_periods(self) -> None:
        """Load experiment periods from JSON file."""
        if self.periods_file.exists():
            try:
                with open(self.periods_file, 'r') as f:
                    self._periods = json.load(f)
                logger.info(f"Loaded {len(self._periods)} experiment periods")
            except Exception as e:
                logger.error(f"Failed to load experiment periods: {e}")
                self._periods = {}
        else:
            logger.info("No existing experiment periods file found")
            self._periods = {}
    
    def _save_periods(self) -> None:
        """Save experiment periods to JSON file."""
        try:
            # Ensure directory exists
            self.periods_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.periods_file, 'w') as f:
                json.dump(self._periods, f, indent=2, sort_keys=True)
            logger.info(f"Saved experiment periods to {self.periods_file}")
        except Exception as e:
            logger.error(f"Failed to save experiment periods: {e}")
            raise
    
    def get_experiment_for_timestamp(self, timestamp: datetime) -> str:
        """
        Get the experiment label for a given timestamp.
        
        Args:
            timestamp: The timestamp to check (should be timezone-aware)
            
        Returns:
            The experiment label for this timestamp
            
        Raises:
            ValueError: If no matching experiment period is found
        """
        # Convert to UTC if timezone-aware, otherwise assume UTC
        if timestamp.tzinfo is not None:
            utc_timestamp = timestamp.astimezone(timezone.utc)
        else:
            utc_timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        # Find the experiment period that contains this timestamp
        for experiment_label, period in self._periods.items():
            start_time = datetime.fromisoformat(period['start_time'])
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            
            # Check if timestamp is after start time
            if utc_timestamp < start_time:
                continue
            
            # Check end time if it exists
            if period.get('end_time'):
                end_time = datetime.fromisoformat(period['end_time'])
                if end_time.tzinfo is None:
                    end_time = end_time.replace(tzinfo=timezone.utc)
                
                if utc_timestamp >= end_time:
                    continue
            
            # This period contains the timestamp
            logger.debug(f"Timestamp {timestamp} matches experiment {experiment_label}")
            return experiment_label
        
        # If no period found, return unknown
        logger.warning(f"No experiment period found for timestamp {timestamp}")
        return "unknown"
    
    def register_new_experiment(
        self, 
        label: str, 
        start_time: datetime, 
        schema_version: str = "v1",
        end_time: Optional[datetime] = None
    ) -> None:
        """
        Register a new experiment period.
        
        Args:
            label: The experiment label
            start_time: When the experiment started
            schema_version: Schema version for this experiment
            end_time: When the experiment ended (None for ongoing)
        """
        # Convert times to UTC ISO format
        if start_time.tzinfo is not None:
            start_utc = start_time.astimezone(timezone.utc)
        else:
            start_utc = start_time.replace(tzinfo=timezone.utc)
        
        period_data = {
            'start_time': start_utc.isoformat(),
            'schema_version': schema_version
        }
        
        if end_time:
            if end_time.tzinfo is not None:
                end_utc = end_time.astimezone(timezone.utc)
            else:
                end_utc = end_time.replace(tzinfo=timezone.utc)
            period_data['end_time'] = end_utc.isoformat()
        
        self._periods[label] = period_data
        self._save_periods()
        logger.info(f"Registered experiment period: {label}")
    
    def close_experiment_period(self, label: str, end_time: datetime) -> None:
        """
        Close an experiment period by setting its end time.
        
        Args:
            label: The experiment label to close
            end_time: When the experiment ended
            
        Raises:
            ValueError: If the experiment doesn't exist
        """
        if label not in self._periods:
            raise ValueError(f"Experiment {label} not found")
        
        # Convert to UTC
        if end_time.tzinfo is not None:
            end_utc = end_time.astimezone(timezone.utc)
        else:
            end_utc = end_time.replace(tzinfo=timezone.utc)
        
        self._periods[label]['end_time'] = end_utc.isoformat()
        self._save_periods()
        logger.info(f"Closed experiment period: {label} at {end_time}")
    
    def get_all_periods(self) -> Dict[str, Dict]:
        """Get all experiment periods."""
        return self._periods.copy()
    
    def validate_periods(self) -> List[str]:
        """
        Validate experiment periods for overlaps or gaps.
        
        Returns:
            List of validation warnings
        """
        warnings = []
        
        # Sort periods by start time
        sorted_periods = []
        for label, period in self._periods.items():
            start_time = datetime.fromisoformat(period['start_time'])
            sorted_periods.append((start_time, label, period))
        
        sorted_periods.sort(key=lambda x: x[0])
        
        # Check for overlaps
        for i in range(len(sorted_periods) - 1):
            current_start, current_label, current_period = sorted_periods[i]
            next_start, next_label, next_period = sorted_periods[i + 1]
            
            # If current period has no end time, it's ongoing
            if not current_period.get('end_time'):
                warnings.append(f"Ongoing experiment {current_label} may overlap with {next_label}")
                continue
            
            current_end = datetime.fromisoformat(current_period['end_time'])
            
            # Check for overlap
            if current_end > next_start:
                warnings.append(f"Overlap detected: {current_label} ends after {next_label} starts")
        
        return warnings


def create_initial_periods_from_data(parquet_path: Path, periods_file: Path) -> ExperimentPeriodManager:
    """
    Create initial experiment periods based on existing data.
    
    This is a helper function for migration from the old system.
    
    Args:
        parquet_path: Path to existing parquet file
        periods_file: Path where periods should be saved
        
    Returns:
        ExperimentPeriodManager with periods based on data
    """
    import pandas as pd
    
    manager = ExperimentPeriodManager(periods_file)
    
    if not parquet_path.exists():
        logger.warning(f"Parquet file not found: {parquet_path}")
        return manager
    
    try:
        df = pd.read_parquet(parquet_path)
        logger.info(f"Loaded {len(df)} rows from {parquet_path}")
        
        # Parse timestamps
        def parse_timestamp(ts_str):
            if ":" in ts_str and ts_str.count(":") > 2:
                ts_str = ts_str.replace(":", "-", 2)
            return datetime.fromisoformat(ts_str.rstrip('Z')).replace(tzinfo=timezone.utc)
        
        df['_parsed_timestamp'] = df['synced_at'].apply(parse_timestamp)
        
        # Group by experiment label and find time ranges
        for exp_label in df['experiment_label'].unique():
            exp_df = df[df['experiment_label'] == exp_label]
            min_time = exp_df['_parsed_timestamp'].min()
            max_time = exp_df['_parsed_timestamp'].max()
            
            # Get schema version (default to v1)
            schema_version = exp_df['schema_version'].iloc[0] if 'schema_version' in exp_df.columns else 'v1'
            
            logger.info(f"Found experiment {exp_label}: {min_time} to {max_time} ({len(exp_df)} rows)")
            
            # Don't set end_time for the most recent experiment (assume it's ongoing)
            end_time = None
            if min_time != df['_parsed_timestamp'].max():
                end_time = max_time
            
            manager.register_new_experiment(
                label=exp_label,
                start_time=min_time,
                schema_version=schema_version,
                end_time=end_time
            )
        
        return manager
        
    except Exception as e:
        logger.error(f"Failed to analyze existing data: {e}")
        return manager


def convert_cet_to_utc(cet_timestamp_str: str) -> datetime:
    """
    Convert CET timestamp string to UTC datetime.
    
    Args:
        cet_timestamp_str: Timestamp string in CET (YYYY-MM-DDTHH:MM:SS)
        
    Returns:
        UTC datetime object
    """
    from datetime import timezone, timedelta
    
    # Parse the timestamp
    dt = datetime.fromisoformat(cet_timestamp_str)
    
    # Add CET timezone (UTC+1)
    cet_tz = timezone(timedelta(hours=1))
    cet_dt = dt.replace(tzinfo=cet_tz)
    
    # Convert to UTC
    utc_dt = cet_dt.astimezone(timezone.utc)
    
    return utc_dt


def main():
    """CLI interface for experiment period management."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage experiment periods")
    parser.add_argument(
        "--periods-file",
        type=Path,
        default=Path(__file__).parent.parent / "data" / "experiment_periods.json",
        help="Path to experiment periods JSON file"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List periods command
    list_parser = subparsers.add_parser('list', help='List all experiment periods')
    
    # Add period command
    add_parser = subparsers.add_parser('add', help='Add a new experiment period')
    add_parser.add_argument('--label', required=True, help='Experiment label')
    add_parser.add_argument('--start-time', required=True, help='Start time (ISO format)')
    add_parser.add_argument('--end-time', help='End time (ISO format)')
    add_parser.add_argument('--schema-version', default='v1', help='Schema version')
    
    # Close period command
    close_parser = subparsers.add_parser('close', help='Close an experiment period')
    close_parser.add_argument('--label', required=True, help='Experiment label')
    close_parser.add_argument('--end-time', required=True, help='End time (ISO format)')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate experiment periods')
    
    # Initialize from data command
    init_parser = subparsers.add_parser('init-from-data', help='Initialize periods from existing data')
    init_parser.add_argument(
        '--parquet-file',
        type=Path,
        default=Path(__file__).parent.parent / "data" / "curated" / "experiments.parquet",
        help='Path to parquet file'
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    manager = ExperimentPeriodManager(args.periods_file)
    
    if args.command == 'list':
        periods = manager.get_all_periods()
        if not periods:
            print("No experiment periods found")
        else:
            print("Experiment Periods:")
            for label, period in sorted(periods.items()):
                end_str = period.get('end_time', 'ongoing')
                print(f"  {label}: {period['start_time']} to {end_str} (schema: {period['schema_version']})")
    
    elif args.command == 'add':
        start_time = datetime.fromisoformat(args.start_time)
        end_time = datetime.fromisoformat(args.end_time) if args.end_time else None
        manager.register_new_experiment(args.label, start_time, args.schema_version, end_time)
        print(f"Added experiment period: {args.label}")
    
    elif args.command == 'close':
        end_time = datetime.fromisoformat(args.end_time)
        manager.close_experiment_period(args.label, end_time)
        print(f"Closed experiment period: {args.label}")
    
    elif args.command == 'validate':
        warnings = manager.validate_periods()
        if not warnings:
            print("All experiment periods are valid")
        else:
            print("Validation warnings:")
            for warning in warnings:
                print(f"  - {warning}")
    
    elif args.command == 'init-from-data':
        manager = create_initial_periods_from_data(args.parquet_file, args.periods_file)
        print("Initialized experiment periods from existing data")
    
    return 0


if __name__ == "__main__":
    exit(main())