#!/usr/bin/env python3
"""
Data ingestor for Kraken analyzer.

Reads all CSV snapshots, unions columns, deduplicates rows,
and writes a single curated Parquet file for analysis.
"""

import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config(config_path: Path, env_path: Path) -> Dict[str, Any]:
    """Load configuration from YAML file and .env file."""
    # Load environment variables from .env file if it exists
    if env_path.exists():
        load_dotenv(env_path)
        logger.info("Loaded environment variables from .env file")
    else:
        logger.info(".env file not found, using sources.yaml only")

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        # Override with environment variables if they exist
        if "sources" in config and len(config["sources"]) > 0:
            source = config["sources"][0]
            source["experiment_label"] = os.getenv(
                "EXPERIMENT_LABEL", source.get("experiment_label", "unknown")
            )
            source["schema_version"] = os.getenv(
                "SCHEMA_VERSION", source.get("schema_version", "v1")
            )
            source["name"] = os.getenv("SOURCE_NAME", source.get("name", "cloud-11"))

        return config
    except Exception as e:
        logger.error(f"Failed to load config from {config_path}: {e}")
        sys.exit(1)


def get_snapshot_files(snapshots_dir: Path) -> List[Path]:
    """Get all CSV snapshot files sorted by modification time."""
    pattern = "run_results.*.csv"
    files = list(snapshots_dir.glob(pattern))
    # Exclude temp files
    files = [f for f in files if not f.name.startswith(".")]
    files.sort(key=lambda f: f.stat().st_mtime)
    logger.info(f"Found {len(files)} snapshot files")
    return files


def extract_timestamp_from_filename(filepath: Path) -> Optional[str]:
    """Extract UTC timestamp from filename like run_results.2025-09-08T10-15-30Z.csv"""
    try:
        # Extract timestamp part between run_results. and .csv
        name_parts = filepath.stem.split(".")
        if len(name_parts) >= 2:
            timestamp_str = ".".join(name_parts[1:])
            # Convert back to standard ISO format
            return timestamp_str.replace("-", ":")
    except Exception:
        pass
    return None


def normalize_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Attempt to convert obvious numeric columns, handling errors gracefully."""
    df = df.copy()

    # Common numeric column patterns for this domain
    numeric_patterns = ["cost", "latency", "size", "time", "rate", "count", "id"]

    for col in df.columns:
        col_lower = col.lower()
        if any(pattern in col_lower for pattern in numeric_patterns):
            try:
                # Try to convert to numeric, errors become NaN
                df[col] = pd.to_numeric(df[col], errors="coerce")
            except Exception as e:
                logger.debug(f"Could not convert column {col} to numeric: {e}")

    return df


def generate_dedup_key(row: pd.Series, fallback_columns: List[str]) -> str:
    """Generate a deduplication key from row data."""
    # Primary: use experiment IDs if available
    primary_keys = ["experiment_label", "kraken_simulation_id", "ines_simulation_id"]

    key_parts = []
    for key in primary_keys:
        if key in row and pd.notna(row[key]):
            key_parts.append(f"{key}={row[key]}")

    if len(key_parts) >= 2:  # We have enough primary keys
        return "|".join(key_parts)

    # Fallback: hash key parameter columns
    fallback_values = []
    for col in fallback_columns:
        if col in row and pd.notna(row[col]):
            fallback_values.append(f"{col}={row[col]}")

    if fallback_values:
        fallback_str = "|".join(sorted(fallback_values))
        return hashlib.md5(fallback_str.encode()).hexdigest()

    # Last resort: hash all non-metadata columns
    non_metadata_cols = [
        c
        for c in row.index
        if not c.startswith(("experiment_", "schema_", "source", "synced_at"))
    ]
    all_values = []
    for col in non_metadata_cols:
        if pd.notna(row[col]):
            all_values.append(f"{col}={row[col]}")

    if all_values:
        all_str = "|".join(sorted(all_values))
        return hashlib.md5(all_str.encode()).hexdigest()

    return "unknown"


def compute_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute light derived metrics if the required columns exist."""
    df = df.copy()

    # Cost delta (Kraken vs INES)
    if "kraken_cost" in df.columns and "ines_cost" in df.columns:
        df["cost_delta"] = df["kraken_cost"] - df["ines_cost"]
        df["cost_improvement_pct"] = (
            (df["ines_cost"] - df["kraken_cost"]) / df["ines_cost"] * 100
        ).round(2)

    # Latency delta
    if "kraken_latency" in df.columns and "ines_latency" in df.columns:
        df["latency_delta"] = df["kraken_latency"] - df["ines_latency"]
        df["latency_improvement_pct"] = (
            (df["ines_latency"] - df["kraken_latency"]) / df["ines_latency"] * 100
        ).round(2)

    # Performance score (lower cost + lower latency is better)
    if "cost_improvement_pct" in df.columns and "latency_improvement_pct" in df.columns:
        df["performance_score"] = (
            df["cost_improvement_pct"] + df["latency_improvement_pct"]
        ) / 2

    return df


def get_processed_snapshots(output_path: Path) -> set:
    """Get list of snapshots that have already been processed."""
    if not output_path.exists():
        return set()
    
    try:
        existing_df = pd.read_parquet(output_path)
        if "snapshot_file" in existing_df.columns:
            return set(existing_df["snapshot_file"].unique())
    except Exception as e:
        logger.warning(f"Could not read existing parquet file: {e}")
    
    return set()


def process_snapshots(config: Dict[str, Any], snapshots_dir: Path, output_path: Path) -> pd.DataFrame:
    """Process only new snapshot files and append to existing dataset."""
    snapshot_files = get_snapshot_files(snapshots_dir)

    if not snapshot_files:
        logger.warning("No snapshot files found")
        return pd.DataFrame()

    # Get list of already processed snapshots
    processed_snapshots = get_processed_snapshots(output_path)
    new_snapshot_files = [f for f in snapshot_files if f.name not in processed_snapshots]
    
    if not new_snapshot_files:
        logger.info("No new snapshot files to process")
        return pd.DataFrame()
    
    logger.info(f"Found {len(new_snapshot_files)} new snapshots to process (out of {len(snapshot_files)} total)")

    all_dataframes = []
    source_config = config["sources"][0]  # Assuming single source for now

    for filepath in new_snapshot_files:
        try:
            logger.info(f"Processing new snapshot: {filepath.name}")

            # Read CSV
            df = pd.read_csv(filepath)
            logger.debug(
                f"Loaded {len(df)} rows, {len(df.columns)} columns from {filepath.name}"
            )

            # Add metadata columns - use current experiment label for new snapshots
            df["experiment_label"] = source_config["experiment_label"]
            df["schema_version"] = source_config["schema_version"]
            df["source"] = source_config["name"]
            df["snapshot_file"] = filepath.name  # Track which snapshot this came from

            # Extract timestamp from filename
            timestamp = extract_timestamp_from_filename(filepath)
            df["synced_at"] = timestamp or datetime.utcnow().isoformat()

            all_dataframes.append(df)

        except Exception as e:
            logger.error(f"Failed to process {filepath}: {e}")
            continue

    if not all_dataframes:
        logger.info("No new data to process")
        return pd.DataFrame()

    # Union all new dataframes
    logger.info("Unioning new dataframes...")
    combined_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
    logger.info(
        f"New data: {len(combined_df)} rows, {len(combined_df.columns)} columns"
    )

    # Load existing data and combine with new data
    if output_path.exists():
        try:
            existing_df = pd.read_parquet(output_path)
            logger.info(f"Loading existing data: {len(existing_df)} rows")
            combined_df = pd.concat([existing_df, combined_df], ignore_index=True, sort=False)
            logger.info(f"Total combined data: {len(combined_df)} rows")
        except Exception as e:
            logger.warning(f"Could not load existing data: {e}, treating as new dataset")

    # Normalize numeric columns
    logger.info("Normalizing numeric columns...")
    combined_df = normalize_numeric_columns(combined_df)

    # Deduplicate
    logger.info("Deduplicating rows...")
    fallback_columns = config.get("analysis", {}).get("required_columns", [])

    # Generate dedup keys
    combined_df["_dedup_key"] = combined_df.apply(
        lambda row: generate_dedup_key(row, fallback_columns), axis=1
    )

    # Keep the most recent entry for each dedup key
    combined_df["_synced_at_dt"] = pd.to_datetime(
        combined_df["synced_at"], errors="coerce"
    )
    dedup_df = combined_df.sort_values("_synced_at_dt").drop_duplicates(
        subset=["_dedup_key"], keep="last"
    )

    # Clean up temporary columns
    dedup_df = dedup_df.drop(columns=["_dedup_key", "_synced_at_dt"])

    duplicates_removed = len(combined_df) - len(dedup_df)
    logger.info(f"Removed {duplicates_removed} duplicate rows")

    return dedup_df


def validate_required_columns(df: pd.DataFrame, required_columns: List[str]) -> None:
    """Validate that required columns are present and warn if missing."""
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logger.warning(f"Missing required columns: {missing_columns}")
        logger.warning("Some metrics may not be computable in notebooks")
    else:
        logger.info("All required columns are present")


def main():
    """Main ingest process."""
    parser = argparse.ArgumentParser(description="Ingest Kraken experiment data")
    parser.add_argument(
        "--reset", 
        action="store_true", 
        help="Reset the parquet file and reprocess all snapshots from scratch"
    )
    args = parser.parse_args()
    
    # Setup paths
    root_dir = Path(__file__).parent.parent
    config_path = root_dir / "configs" / "sources.yaml"
    env_path = root_dir / ".env"
    snapshots_dir = root_dir / "data" / "raw" / "cloud-11" / "snapshots"
    curated_dir = root_dir / "data" / "curated"
    output_path = curated_dir / "experiments.parquet"

    # Ensure directories exist
    curated_dir.mkdir(parents=True, exist_ok=True)
    
    # Handle reset option
    if args.reset:
        if output_path.exists():
            logger.info("Resetting: removing existing parquet file")
            output_path.unlink()
        else:
            logger.info("Reset requested but no existing parquet file found")

    # Load configuration
    logger.info(f"Loading config from {config_path}")
    config = load_config(config_path, env_path)

    # Process snapshots
    logger.info("Starting data ingestion...")
    df = process_snapshots(config, snapshots_dir, output_path)

    if df.empty:
        if args.reset or not output_path.exists():
            logger.error("No data to write")
            sys.exit(1)
        else:
            logger.info("No new data to process, existing file unchanged")
            return

    # Validate required columns
    required_columns = config.get("analysis", {}).get("required_columns", [])
    validate_required_columns(df, required_columns)

    # Write curated parquet file
    logger.info(f"Writing curated dataset to {output_path}")
    df.to_parquet(output_path, index=False, compression="snappy")

    # Summary stats
    logger.info("Ingestion complete:")
    logger.info(f"  - Final dataset: {len(df)} rows, {len(df.columns)} columns")
    logger.info(
        f"  - Experiments: {df['experiment_label'].nunique() if 'experiment_label' in df.columns else 'Unknown'}"
    )
    logger.info(
        f"  - Schema versions: {df['schema_version'].nunique() if 'schema_version' in df.columns else 'Unknown'}"
    )
    logger.info(
        f"  - Date range: {df['synced_at'].min() if 'synced_at' in df.columns else 'Unknown'} to {df['synced_at'].max() if 'synced_at' in df.columns else 'Unknown'}"
    )
    logger.info(f"  - Output: {output_path}")


if __name__ == "__main__":
    main()
