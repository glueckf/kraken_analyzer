#!/usr/bin/env python3
"""
Script to fix experiment labels in the parquet file based on snapshot timing.

This script helps separate data from different experiments when they were
incorrectly labeled due to the original bug in the ingest system.
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_timestamp(timestamp_str: str) -> datetime:
    """Parse timestamp from various formats."""
    # Handle format like "2025:09:10T06:18:09Z"
    if ":" in timestamp_str and timestamp_str.count(":") > 2:
        # Convert "2025:09:10T06:18:09Z" to "2025-09-10T06:18:09Z"
        timestamp_str = timestamp_str.replace(":", "-", 2)
    
    # Remove 'Z' if present and parse
    timestamp_str = timestamp_str.rstrip('Z')
    return datetime.fromisoformat(timestamp_str)


def main():
    """Main function to fix experiment labels."""
    parser = argparse.ArgumentParser(description="Fix experiment labels based on snapshot timing")
    parser.add_argument(
        "--cutoff-time",
        required=True,
        help="Timestamp cutoff between experiments (ISO format: YYYY-MM-DDTHH:MM:SS)"
    )
    parser.add_argument(
        "--old-label",
        required=True,
        help="Experiment label for snapshots before cutoff"
    )
    parser.add_argument(
        "--new-label", 
        required=True,
        help="Experiment label for snapshots after cutoff"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying the file"
    )
    
    args = parser.parse_args()
    
    # Setup paths
    root_dir = Path(__file__).parent.parent
    parquet_path = root_dir / "data" / "curated" / "experiments.parquet"
    
    if not parquet_path.exists():
        logger.error(f"Parquet file not found: {parquet_path}")
        return 1
    
    # Load data
    logger.info(f"Loading data from {parquet_path}")
    df = pd.read_parquet(parquet_path)
    logger.info(f"Loaded {len(df)} rows")
    
    # Parse cutoff time
    try:
        cutoff_time = datetime.fromisoformat(args.cutoff_time)
        logger.info(f"Using cutoff time: {cutoff_time}")
    except ValueError as e:
        logger.error(f"Invalid cutoff time format: {e}")
        return 1
    
    # Convert synced_at to datetime for comparison
    df['_synced_at_dt'] = df['synced_at'].apply(parse_timestamp)
    
    # Show current state
    logger.info("Current experiment labels:")
    print(df['experiment_label'].value_counts())
    
    logger.info("Snapshot timing:")
    timing_df = df.groupby(['snapshot_file', 'experiment_label']).agg({
        '_synced_at_dt': ['min', 'max', 'count']
    }).reset_index()
    timing_df.columns = ['snapshot_file', 'experiment_label', 'min_time', 'max_time', 'count']
    timing_df = timing_df.sort_values('min_time')
    print(timing_df)
    
    # Create new labels based on cutoff
    new_labels = []
    for idx, row in df.iterrows():
        if row['_synced_at_dt'] < cutoff_time:
            new_labels.append(args.old_label)
        else:
            new_labels.append(args.new_label)
    
    # Show what would change
    df['_new_experiment_label'] = new_labels
    changes_df = df.groupby(['experiment_label', '_new_experiment_label']).size().reset_index(name='count')
    
    logger.info("Proposed changes:")
    print(changes_df)
    
    if args.dry_run:
        logger.info("Dry run - no changes made")
        return 0
    
    # Apply changes
    df['experiment_label'] = df['_new_experiment_label']
    df = df.drop(columns=['_synced_at_dt', '_new_experiment_label'])
    
    # Save updated file
    logger.info(f"Saving updated data to {parquet_path}")
    df.to_parquet(parquet_path, index=False, compression="snappy")
    
    # Show final state
    logger.info("Final experiment labels:")
    print(df['experiment_label'].value_counts())
    
    logger.info("Fix complete!")
    return 0


if __name__ == "__main__":
    exit(main())