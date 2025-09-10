#!/usr/bin/env python3
"""
Enhanced script to fix experiment labels in the parquet file based on snapshot timing.

This script helps separate data from different experiments when they were
incorrectly labeled due to the original bug in the ingest system. It now
integrates with the experiment period tracking system for future-proof labeling.
"""

import argparse
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Optional
import shutil

import pandas as pd

# Import our experiment management utilities
from experiment_manager import ExperimentPeriodManager, convert_cet_to_utc

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_timestamp(timestamp_str: str) -> datetime:
    """Parse timestamp from various formats and return UTC datetime."""
    # Handle format like "2025:09:10T06:18:09Z"  
    if ":" in timestamp_str and timestamp_str.count(":") > 2:
        # Convert "2025:09:10T06:18:09Z" to "2025-09-10T06:18:09Z"
        timestamp_str = timestamp_str.replace(":", "-", 2)
    
    # Remove 'Z' if present and parse
    timestamp_str = timestamp_str.rstrip('Z')
    dt = datetime.fromisoformat(timestamp_str)
    
    # Assume UTC if no timezone info
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    return dt


def backup_parquet_file(parquet_path: Path) -> Path:
    """Create a backup of the parquet file before modifying."""
    backup_path = parquet_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
    shutil.copy2(parquet_path, backup_path)
    logger.info(f"Created backup: {backup_path}")
    return backup_path


def fix_labels_with_cutoff(
    parquet_path: Path,
    cutoff_time: datetime,
    old_label: str,
    new_label: str,
    dry_run: bool = True
) -> Dict[str, Any]:
    """
    Fix experiment labels using a simple cutoff time approach.
    
    Returns dict with statistics about the changes.
    """
    # Load data
    logger.info(f"Loading data from {parquet_path}")
    df = pd.read_parquet(parquet_path)
    logger.info(f"Loaded {len(df)} rows")
    
    # Parse timestamps
    df['_synced_at_dt'] = df['synced_at'].apply(parse_timestamp)
    
    # Show current state
    logger.info("Current experiment labels:")
    current_counts = df['experiment_label'].value_counts().to_dict()
    for label, count in current_counts.items():
        print(f"  {label}: {count}")
    
    # Show timing analysis
    logger.info(f"Using cutoff time: {cutoff_time}")
    before_cutoff = df[df['_synced_at_dt'] < cutoff_time]
    after_cutoff = df[df['_synced_at_dt'] >= cutoff_time]
    
    logger.info(f"Snapshots before {cutoff_time}: {len(before_cutoff)}")
    logger.info(f"Snapshots after {cutoff_time}: {len(after_cutoff)}")
    
    # Create new labels based on cutoff
    new_labels = []
    for _, row in df.iterrows():
        if row['_synced_at_dt'] < cutoff_time:
            new_labels.append(old_label)
        else:
            new_labels.append(new_label)
    
    # Show what would change
    df['_new_experiment_label'] = new_labels
    changes_df = df.groupby(['experiment_label', '_new_experiment_label']).size().reset_index(name='count')
    
    logger.info("Proposed changes:")
    for _, row in changes_df.iterrows():
        print(f"  {row['experiment_label']} -> {row['_new_experiment_label']}: {row['count']} rows")
    
    if dry_run:
        logger.info("Dry run - no changes made")
        return {
            'old_counts': current_counts,
            'new_counts': {old_label: len(before_cutoff), new_label: len(after_cutoff)},
            'total_rows': len(df),
            'dry_run': True
        }
    
    # Create backup before modifying
    backup_path = backup_parquet_file(parquet_path)
    
    # Apply changes
    df['experiment_label'] = df['_new_experiment_label']
    df = df.drop(columns=['_synced_at_dt', '_new_experiment_label'])
    
    # Save updated file
    logger.info(f"Saving updated data to {parquet_path}")
    df.to_parquet(parquet_path, index=False, compression="snappy")
    
    # Show final state
    final_counts = df['experiment_label'].value_counts().to_dict()
    logger.info("Final experiment labels:")
    for label, count in final_counts.items():
        print(f"  {label}: {count}")
    
    logger.info("Fix complete!")
    return {
        'old_counts': current_counts,
        'new_counts': final_counts,
        'total_rows': len(df),
        'backup_file': str(backup_path),
        'dry_run': False
    }


def fix_labels_with_periods(
    parquet_path: Path,
    periods_manager: ExperimentPeriodManager,
    dry_run: bool = True
) -> Dict[str, Any]:
    """
    Fix experiment labels using the experiment period tracking system.
    
    Returns dict with statistics about the changes.
    """
    # Load data
    logger.info(f"Loading data from {parquet_path}")
    df = pd.read_parquet(parquet_path)
    logger.info(f"Loaded {len(df)} rows")
    
    # Parse timestamps
    df['_synced_at_dt'] = df['synced_at'].apply(parse_timestamp)
    
    # Show current state
    logger.info("Current experiment labels:")
    current_counts = df['experiment_label'].value_counts().to_dict()
    for label, count in current_counts.items():
        print(f"  {label}: {count}")
    
    # Assign new labels based on periods
    new_labels = []
    for _, row in df.iterrows():
        new_label = periods_manager.get_experiment_for_timestamp(row['_synced_at_dt'])
        new_labels.append(new_label)
    
    # Show what would change  
    df['_new_experiment_label'] = new_labels
    changes_df = df.groupby(['experiment_label', '_new_experiment_label']).size().reset_index(name='count')
    
    logger.info("Proposed changes:")
    for _, row in changes_df.iterrows():
        print(f"  {row['experiment_label']} -> {row['_new_experiment_label']}: {row['count']} rows")
    
    new_counts = df['_new_experiment_label'].value_counts().to_dict()
    
    if dry_run:
        logger.info("Dry run - no changes made")
        return {
            'old_counts': current_counts,
            'new_counts': new_counts,
            'total_rows': len(df),
            'dry_run': True
        }
    
    # Create backup before modifying
    backup_path = backup_parquet_file(parquet_path)
    
    # Apply changes
    df['experiment_label'] = df['_new_experiment_label']
    df = df.drop(columns=['_synced_at_dt', '_new_experiment_label'])
    
    # Save updated file
    logger.info(f"Saving updated data to {parquet_path}")
    df.to_parquet(parquet_path, index=False, compression="snappy")
    
    # Show final state
    final_counts = df['experiment_label'].value_counts().to_dict()
    logger.info("Final experiment labels:")
    for label, count in final_counts.items():
        print(f"  {label}: {count}")
    
    logger.info("Fix complete!")
    return {
        'old_counts': current_counts,
        'new_counts': final_counts,
        'total_rows': len(df),
        'backup_file': str(backup_path),
        'dry_run': False
    }


def main():
    """Enhanced main function with multiple fix strategies."""
    parser = argparse.ArgumentParser(
        description="Fix experiment labels based on snapshot timing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fix using cutoff time (original method)
  python fix_experiment_labels.py cutoff --cutoff-time 2025-09-10T06:00:00 --old-label kraken1.0_vs_INES --new-label kraken1.1_vs_INES --dry-run
  
  # Fix using CET cutoff time (converts to UTC automatically)
  python fix_experiment_labels.py cutoff --cutoff-time 2025-09-10T07:00:00 --timezone CET --old-label kraken1.0_vs_INES --new-label kraken1.1_vs_INES
  
  # Fix using experiment periods (future-proof method)
  python fix_experiment_labels.py periods --periods-file data/experiment_periods.json --dry-run
  
  # Initialize periods from current data before fixing
  python fix_experiment_labels.py init-periods --cutoff-time 2025-09-10T06:00:00 --old-label kraken1.0_vs_INES --new-label kraken1.1_vs_INES
        """
    )
    
    # Setup common arguments
    parser.add_argument(
        "--parquet-file",
        type=Path,
        help="Path to parquet file (default: data/curated/experiments.parquet)"
    )
    
    # Create subcommands
    subparsers = parser.add_subparsers(dest='command', help='Fix strategy to use')
    
    # Cutoff-based fix (original method)
    cutoff_parser = subparsers.add_parser('cutoff', help='Fix labels using a cutoff time')
    cutoff_parser.add_argument(
        "--cutoff-time",
        required=True,
        help="Timestamp cutoff between experiments (ISO format: YYYY-MM-DDTHH:MM:SS)"
    )
    cutoff_parser.add_argument(
        "--old-label",
        required=True,
        help="Experiment label for snapshots before cutoff"
    )
    cutoff_parser.add_argument(
        "--new-label", 
        required=True,
        help="Experiment label for snapshots after cutoff"
    )
    cutoff_parser.add_argument(
        "--timezone",
        choices=['UTC', 'CET'],
        default='UTC',
        help="Timezone of the cutoff time (default: UTC)"
    )
    cutoff_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying the file"
    )
    
    # Period-based fix (future-proof method)
    periods_parser = subparsers.add_parser('periods', help='Fix labels using experiment periods file')
    periods_parser.add_argument(
        "--periods-file",
        type=Path,
        help="Path to experiment periods JSON file (default: data/experiment_periods.json)"
    )
    periods_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying the file"
    )
    
    # Initialize periods command
    init_parser = subparsers.add_parser('init-periods', help='Initialize periods file and fix labels')
    init_parser.add_argument(
        "--cutoff-time",
        required=True,
        help="Timestamp cutoff between experiments (ISO format: YYYY-MM-DDTHH:MM:SS)"
    )
    init_parser.add_argument(
        "--old-label",
        required=True,
        help="Experiment label for snapshots before cutoff"
    )
    init_parser.add_argument(
        "--new-label", 
        required=True,
        help="Experiment label for snapshots after cutoff"
    )
    init_parser.add_argument(
        "--timezone",
        choices=['UTC', 'CET'],
        default='UTC',
        help="Timezone of the cutoff time (default: UTC)"
    )
    init_parser.add_argument(
        "--periods-file",
        type=Path,
        help="Path to experiment periods JSON file (default: data/experiment_periods.json)"
    )
    init_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying the file"
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Setup paths
    root_dir = Path(__file__).parent.parent
    
    if args.parquet_file:
        parquet_path = args.parquet_file
    else:
        parquet_path = root_dir / "data" / "curated" / "experiments.parquet"
    
    if not parquet_path.exists():
        logger.error(f"Parquet file not found: {parquet_path}")
        return 1
    
    try:
        if args.command == 'cutoff':
            # Parse cutoff time with timezone handling
            cutoff_time = datetime.fromisoformat(args.cutoff_time)
            
            if args.timezone == 'CET':
                cutoff_time = convert_cet_to_utc(args.cutoff_time)
                logger.info(f"Converted CET cutoff {args.cutoff_time} to UTC: {cutoff_time}")
            else:
                # Assume UTC if no timezone info
                if cutoff_time.tzinfo is None:
                    cutoff_time = cutoff_time.replace(tzinfo=timezone.utc)
            
            result = fix_labels_with_cutoff(
                parquet_path=parquet_path,
                cutoff_time=cutoff_time,
                old_label=args.old_label,
                new_label=args.new_label,
                dry_run=args.dry_run
            )
            
        elif args.command == 'periods':
            # Setup periods file path
            if args.periods_file:
                periods_file = args.periods_file
            else:
                periods_file = root_dir / "data" / "experiment_periods.json"
            
            periods_manager = ExperimentPeriodManager(periods_file)
            
            result = fix_labels_with_periods(
                parquet_path=parquet_path,
                periods_manager=periods_manager,
                dry_run=args.dry_run
            )
            
        elif args.command == 'init-periods':
            # Setup periods file path
            if args.periods_file:
                periods_file = args.periods_file
            else:
                periods_file = root_dir / "data" / "experiment_periods.json"
            
            # Parse cutoff time with timezone handling
            cutoff_time = datetime.fromisoformat(args.cutoff_time)
            
            if args.timezone == 'CET':
                cutoff_time = convert_cet_to_utc(args.cutoff_time)
                logger.info(f"Converted CET cutoff {args.cutoff_time} to UTC: {cutoff_time}")
            else:
                # Assume UTC if no timezone info
                if cutoff_time.tzinfo is None:
                    cutoff_time = cutoff_time.replace(tzinfo=timezone.utc)
            
            # Initialize periods manager and create initial periods
            periods_manager = ExperimentPeriodManager(periods_file)
            
            # Create periods based on cutoff
            # Old experiment period (before cutoff)
            periods_manager.register_new_experiment(
                label=args.old_label,
                start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),  # Arbitrary early start
                end_time=cutoff_time,
                schema_version='v1'
            )
            
            # New experiment period (after cutoff, ongoing)
            periods_manager.register_new_experiment(
                label=args.new_label,
                start_time=cutoff_time,
                schema_version='v1'
            )
            
            logger.info(f"Created experiment periods in {periods_file}")
            
            # Now fix labels using the periods
            result = fix_labels_with_periods(
                parquet_path=parquet_path,
                periods_manager=periods_manager,
                dry_run=args.dry_run
            )
        
        # Display summary
        if not result['dry_run']:
            print("\n=== SUMMARY ===")
            print(f"Total rows processed: {result['total_rows']}")
            print(f"Backup created: {result.get('backup_file', 'N/A')}")
            print("Label changes:")
            for label, count in result['new_counts'].items():
                old_count = result['old_counts'].get(label, 0)
                if old_count != count:
                    print(f"  {label}: {old_count} -> {count} ({count - old_count:+d})")
                else:
                    print(f"  {label}: {count} (unchanged)")
            
            # Validate expected counts for the specific kraken fix
            if (args.command in ['cutoff', 'init-periods'] and 
                args.old_label == 'kraken1.0_vs_INES' and 
                args.new_label == 'kraken1.1_vs_INES'):
                
                expected_old = 1200
                expected_new = 1051
                actual_old = result['new_counts'].get('kraken1.0_vs_INES', 0)
                actual_new = result['new_counts'].get('kraken1.1_vs_INES', 0)
                
                print(f"\n=== VALIDATION ===")
                print(f"Expected kraken1.0_vs_INES: {expected_old}, got: {actual_old}")
                print(f"Expected kraken1.1_vs_INES: {expected_new}, got: {actual_new}")
                
                if actual_old == expected_old and actual_new == expected_new:
                    print("SUCCESS: Counts match expected values!")
                else:
                    print("ERROR: Counts do not match expected values")
                    return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"Failed to fix experiment labels: {e}")
        return 1


if __name__ == "__main__":
    exit(main())