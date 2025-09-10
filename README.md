# Kraken Analyzer

**Advanced experiment management system** for Kraken Placement Engine experiments with robust data integrity, experiment separation, and future-proof architecture.

## Key Features

- **Experiment Separation**: Automatically labels data by timestamp, not current settings
- **Timezone Support**: Handles both UTC and CET with automatic conversion
- **Future-Proof**: New experiments are automatically labeled correctly
- **Data Safety**: Comprehensive backup system and data integrity validation
- **One-Click Workflow**: `make analyze` pulls, processes, and prepares data for analysis
- **Schema Evolution**: Handles changing CSV formats gracefully

## Quick Start

```bash
# 1. Configure your environment  
cp .env.example .env
# Edit .env with your server details

# 2. Install dependencies
make install-deps

# 3. One-click analysis
make analyze

# 4. Open analysis notebook
jupyter notebook notebooks/01_overview.ipynb
```

## Architecture Overview

```
[Server CSV] ---(sync)---> [Timestamped Snapshots] ---(ingest)---> [Curated Parquet]
                                    |
                                    v
                          [Experiment-Aware Backups]
                                    ^
                                    |
                            [Period Tracking System]
```

### Directory Structure

```
kraken_analyzer/
├── .env                           # Environment configuration (private)
├── data/
│   ├── experiment_periods.json   # Experiment period definitions
│   ├── raw/cloud-11/
│   │   ├── snapshots/            # Timestamped CSV files  
│   │   └── latest.csv            # Symlink to newest snapshot
│   └── curated/
│       └── experiments.parquet   # Unified dataset for analysis
├── scripts/
│   ├── experiment_manager.py     # Experiment period management
│   ├── fix_experiment_labels.py  # Label correction utilities
│   ├── timezone_utils.py         # Timezone conversion tools
│   ├── sync_server.sh            # Server synchronization
│   └── ingest.py                 # Data processing pipeline
├── notebooks/
│   └── 01_overview.ipynb         # Analysis examples
└── logs/                         # Sync logs and diagnostics
```

## Configuration

### Environment Variables (.env)

```bash
# Server Connection
SSH_USER=your-username
SSH_HOST=your-server.example.com
REMOTE_PATH=/path/to/run_results.csv

# Current Experiment (for new data)
EXPERIMENT_LABEL=kraken1.1_vs_INES
SCHEMA_VERSION=v1

# Backup Configuration
ICLOUD_BACKUP=true
ICLOUD_DEST=/Users/your-username/Desktop/Experiment_Archive/${EXPERIMENT_LABEL}
```

## Essential Commands

### Core Workflow
- `make analyze` - **One-click**: sync + ingest + ready for analysis
- `make sync` - Pull latest data from server  
- `make ingest` - Process snapshots into curated dataset
- `make status` - Show comprehensive system status

### Experiment Management
- `make experiment-status` - View experiment breakdown and data counts
- `make fix-labels-quick CUTOFF=... OLD=... NEW=...` - Fix mislabeled experiments
- `make experiment-periods` - Show experiment period configuration

### Diagnostics & Maintenance
- `make schema-status` - View schema versions and parquet files
- `make backup-status` - Check backup configuration
- `make timezone-info` - Current timezone information  
- `make clean` - Clean temporary files (preserves all data)

## Experiment Management

### The Labeling Problem (Solved!)

**Previous Issue**: All snapshots were labeled with the current `EXPERIMENT_LABEL` from `.env`, causing historical data corruption.

**Solution**: The system now uses **timestamp-based labeling**:
- Each snapshot gets labeled based on when it was created, not current settings
- Historical data integrity is preserved
- Future experiments are automatically handled correctly

### Experiment Periods System

The system tracks experiment periods in `data/experiment_periods.json`:

```json
{
  "kraken1.0_vs_INES": {
    "start_time": "2025-01-01T00:00:00+00:00",
    "end_time": "2025-09-10T08:00:00+00:00",
    "schema_version": "v1"
  },
  "kraken1.1_vs_INES": {
    "start_time": "2025-09-10T08:00:00+00:00", 
    "schema_version": "v1"
  }
}
```

### Starting a New Experiment

1. **Update `.env`** with new experiment label:
   ```bash
   EXPERIMENT_LABEL=kraken2.0_vs_INES
   ```

2. **Continue normal workflow** - the system handles the rest:
   ```bash
   make analyze
   ```

3. **Verify experiment separation**:
   ```bash
   make experiment-status
   ```

### Fixing Mislabeled Data

If experiments were mislabeled due to the old system:

```bash
# Quick fix for kraken1.0 vs kraken1.1 separation
make fix-labels-quick CUTOFF=2025-09-10T08:00:00 OLD=kraken1.0_vs_INES NEW=kraken1.1_vs_INES

# For other experiments, adjust the parameters:
make fix-labels-quick CUTOFF=YYYY-MM-DDTHH:MM:SS OLD=old_label NEW=new_label
```

The system will:
- Show a dry run first
- Ask for confirmation  
- Create automatic backups
- Validate the results

## Timezone Management

The system standardizes on **CET (Central European Time)** for operations while storing everything in **UTC**:

- **Snapshots**: Always timestamped in UTC
- **Cutoffs**: Can be specified in either UTC or CET  
- **Analysis**: Timezone-aware throughout
- **Conversion**: Automatic UTC ↔ CET conversion

### Timezone Utilities

```bash
# Convert timestamps
python3 scripts/timezone_utils.py convert 2025-09-10T08:00:00 --from-tz UTC --to-tz CET

# Parse complex timestamps  
python3 scripts/timezone_utils.py parse 2025:09:10T06:18:09Z

# Current time in both zones
make timezone-info
```

## Data Safety & Backup System

### Automatic Backups

The system creates **experiment-aware backups**:

```
Backup Structure:
├── kraken1.0_vs_INES/
│   ├── latest.csv
│   └── run_results.2025-09-10T06-18-09Z.csv
├── kraken1.1_vs_INES/  
│   ├── latest.csv
│   └── run_results.2025-09-10T09-18-13Z.csv
└── [legacy compatibility folders]
```

### Data Integrity Features

- **Automatic backups** before any label changes
- **Comprehensive validation** of experiment counts
- **Incremental processing** - only new snapshots are processed
- **Schema evolution** - handles new CSV columns gracefully
- **Atomic operations** - never corrupts existing data

## Analysis Examples

### Basic Analysis

```python
import pandas as pd

# Load unified dataset
df = pd.read_parquet('data/curated/experiments.parquet')

# View experiment breakdown
print("Available experiments:")
print(df['experiment_label'].value_counts())

# Filter to specific experiment  
kraken10_data = df[df['experiment_label'] == 'kraken1.0_vs_INES']
kraken11_data = df[df['experiment_label'] == 'kraken1.1_vs_INES']

# Compare experiments
comparison = df.groupby('experiment_label')[['kraken_cost', 'ines_cost']].mean()
print(comparison)
```

### Advanced Filtering

```python
# Time-based analysis
df['timestamp'] = pd.to_datetime(df['synced_at'])
df['date'] = df['timestamp'].dt.date

# Daily experiment progress
daily_counts = df.groupby(['date', 'experiment_label']).size()
print(daily_counts)

# Schema evolution tracking
schema_usage = df.groupby('schema_version')['experiment_label'].value_counts()
print(schema_usage)
```

## Troubleshooting

### Common Issues

**No snapshots being created:**
```bash
make logs                    # Check sync logs
make backup-status          # Verify backup configuration  
ssh $SSH_USER@$SSH_HOST     # Test connection
```

**Experiment labels look wrong:**
```bash
make experiment-status      # Check current distribution
make experiment-periods     # View period configuration
make experiment-validate    # Check for period issues
```

**Missing curated data:**
```bash
ls data/raw/cloud-11/snapshots/  # Verify raw snapshots exist
make ingest                      # Rerun processing
make schema-status              # Check schema compatibility
```

**Backup issues:**
```bash
make backup-status          # Check backup configuration
make timezone-info          # Verify timezone handling
```

### Emergency Procedures

**Completely reset system (WARNING: Dangerous):**
```bash
make ingest-reset           # Rebuilds from all snapshots
```

**Restore from backup:**
```bash
cp data/curated/experiments.backup_TIMESTAMP.parquet data/curated/experiments.parquet
```

## Advanced Features

### Schema Migration

When CSV format changes:

1. **Update schema version** in `.env`:
   ```bash  
   SCHEMA_VERSION=v2
   ```

2. **Continue normal workflow** - system tracks multiple schemas:
   ```bash
   make analyze
   ```

3. **Monitor schema usage**:
   ```bash
   make schema-status
   ```

### Custom Period Management

```bash
# View all periods
python3 scripts/experiment_manager.py list

# Add new period
python3 scripts/experiment_manager.py add --label "kraken2.0_vs_INES" --start-time "2025-10-01T00:00:00Z"

# Close period
python3 scripts/experiment_manager.py close --label "kraken1.1_vs_INES" --end-time "2025-09-30T23:59:59Z"

# Validate periods
python3 scripts/experiment_manager.py validate
```

### Automation (Optional)

```bash
# Start automated sync every 60 minutes
make start-scheduler

# Check scheduler status
make scheduler-status

# Stop automation
make stop-scheduler
```

## Performance

- **Sync**: ~30-60 seconds for 10MB CSV
- **Ingestion**: ~5-15 seconds for 100K rows  
- **Analysis**: ~1-3 seconds to load curated Parquet
- **Memory**: Processes datasets up to 1M+ rows efficiently

## Development

### Adding New Metrics

1. Edit `scripts/ingest.py` → `compute_derived_metrics()`
2. Add column existence checks in notebooks
3. Test with multiple schema versions

### New Analysis Workflows

1. Copy `notebooks/01_overview.ipynb` as template
2. Always check column existence:
   ```python
   if 'my_column' in df.columns:
       # compute metric
   else:
       print("Column not available in this schema version")
   ```

## Migration from Old System

If upgrading from the old system:

1. **Current data is already fixed**
2. **Experiment periods are configured**  
3. **Future experiments will work correctly**
4. **Backups are organized by experiment**

No manual migration required - the system is ready to use!

## Support

**View system status:**
```bash
make status
make experiment-status  
make experiment-validate
```

**Get help with commands:**
```bash
make                    # Show available commands
make fix-labels        # Show labeling strategies
```

**Debug specific issues:**
```bash
make logs              # Recent sync logs
make schema-status     # Schema compatibility  
make backup-status     # Backup configuration
```

## License

MIT License - see LICENSE file for details.