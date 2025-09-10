# Kraken Analyzer

A one-click, reproducible analyzer for the Kraken Placement Engine experiments that safely pulls data from the server, creates local snapshots, and builds a curated dataset for analysis.

## Quick Start

```bash
# 1. Copy environment template and configure your settings
cp .env.example .env
# Edit .env with server details and paths

# 2. Install dependencies
make install-deps

# 3. One-click analysis (pull + ingest)  
make analyze

# 4. Open Jupyter notebook
jupyter notebook notebooks/01_overview.ipynb
```

## Features

- **Safe server pulls**: Read-only rsync with stability checks
- **Automatic snapshots**: Timestamped CSV files with iCloud backup
- **Schema evolution**: Handles new columns gracefully 
- **Deduplication**: Intelligent row deduplication by experiment IDs
- **Automated scheduling**: Optional 60-minute updates via launchd
- **Backwards compatible**: Works with changing server schemas

## Architecture

```
[Server CSV] ---(pull)---> [Local Snapshots] ---(ingest)---> [Curated Parquet]
                               |
                               v
                         [iCloud Backup]
```

## Directory Structure

```
kraken_analyzer/
├── configs/sources.yaml        # Server config & experiment labels
├── data/
│   ├── raw/cloud-11/
│   │   ├── snapshots/          # Timestamped CSV files
│   │   └── latest.csv          # Symlink to newest snapshot
│   └── curated/
│       └── experiments.parquet # Single source for analysis
├── scripts/
│   ├── sync_server.sh         # Safe server pull script
│   └── ingest.py              # Data processing & curation
├── notebooks/
│   └── 01_overview.ipynb      # Example analysis notebook
├── launchd/                   # Automated scheduling
└── logs/                      # Sync logs and lock files
```

## Configuration

### Environment Variables (.env)

The system uses environment variables for sensitive configuration. Copy `.env.example` to `.env` and configure:

```bash
# Server Connection (keep private)
SSH_USER=your-username
SSH_HOST=your-server.example.com
REMOTE_PATH=/path/to/your/run_results.csv

# Experiment Configuration  
EXPERIMENT_LABEL=kraken1.0_vs_INES  # Change this for new experiments
SCHEMA_VERSION=v1

# iCloud Backup
ICLOUD_BACKUP=true
ICLOUD_DEST=/Users/your-username/Desktop/Bachelorarbeit/Simulation_Runs_Archive
```

### Legacy Configuration (configs/sources.yaml)

The system still supports YAML configuration for backwards compatibility, but `.env` takes precedence if present.

### Setup Steps

1. **Clone and configure**:
   ```bash
   git clone <repository>
   cd kraken_analyzer
   cp .env.example .env
   # Edit .env with your server details
   ```

2. **Install dependencies**:
   ```bash
   make install-deps
   ```

3. **Test connection**:
   ```bash
   make sync  # Should create first snapshot
   ```

4. **First analysis**:
   ```bash
   make analyze  # Pull + ingest + ready for notebooks
   ```

## Commands

### Core Operations
- `make analyze` - One-click: pull data + ingest + ready for analysis
- `make sync` - Pull data from server only  
- `make ingest` - Process snapshots into curated Parquet (incremental)
- `make status` - Show current data status

### Experiment Management
- `make experiment-status` - Show current experiment data breakdown
- `make ingest-reset` - Reset parquet file and reprocess all snapshots from scratch
- `make fix-labels` - Fix experiment labels when data was incorrectly labeled

### Automation
- `make start-scheduler` - Start automated updates (every 60 min)
- `make stop-scheduler` - Stop automated updates
- `make scheduler-status` - Check if scheduler is running

### Maintenance
- `make logs` - Show recent sync logs
- `make clean` - Clean temporary files (keeps data), remove sync locks from previous runs

## Data Flow

### 1. Server Pull (`make sync`)
- Connects to server via SSH (read-only)
- Downloads `run_results.csv` with rsync (resumable)
- Verifies file stability (waits for writes to finish)
- Creates timestamped local snapshot: `run_results.2025-09-08T10-15-30Z.csv`
- Updates `latest.csv` symlink
- Mirrors to iCloud backup folder

### 2. Data Ingestion (`make ingest`)
- **Incremental processing**: Only processes new snapshots that haven't been ingested yet
- Preserves existing experiment labels (no overwriting of old data)
- Unions all columns (handles schema evolution)
- Normalizes numeric columns where possible
- Deduplicates by `(experiment_label, kraken_simulation_id, ines_simulation_id)`
- Adds metadata: experiment_label, schema_version, source, synced_at, snapshot_file
- Computes derived metrics (cost/latency deltas and improvements)
- Appends to or creates `experiments.parquet` file

### 3. Analysis
- Notebooks read from `data/curated/experiments.parquet`
- All metrics check column existence before computing
- Safe to run with any schema version

## Schema Evolution

The system handles schema changes gracefully:

- **New columns**: Automatically included in curated dataset
- **Missing columns**: Notebooks skip metrics that need missing columns
- **Schema versions**: Track different formats with `schema_version` field

**When starting a new experiment**: Change only `EXPERIMENT_LABEL` in `.env`

**When CSV format truly changes**: Bump `SCHEMA_VERSION` to track different formats

## Managing Multiple Experiments

The system now supports running multiple experiments safely without data corruption:

### Starting a New Experiment

1. **Update experiment label** in `.env`:
   ```bash
   EXPERIMENT_LABEL=kraken2.0_vs_INES  # Change to new experiment name
   ```

2. **Continue normal workflow**:
   ```bash
   make analyze  # New data will be labeled with new experiment label
   ```

3. **View experiment breakdown**:
   ```bash
   make experiment-status
   ```

### Filtering in Notebooks

Filter by experiment in your analysis:
```python
import pandas as pd

df = pd.read_parquet('data/curated/experiments.parquet')

# Filter to specific experiment
kraken1_data = df[df['experiment_label'] == 'kraken1.0_vs_INES']
kraken2_data = df[df['experiment_label'] == 'kraken2.0_vs_INES']

# Show experiment breakdown
print("Available experiments:")
print(df['experiment_label'].value_counts())
```

### Fixing Mislabeled Data

If you accidentally changed the experiment label mid-experiment and need to fix the data:

```bash
# Show current status first
make experiment-status

# Fix labels with a timestamp cutoff
make fix-labels CUTOFF=2025-09-10T06:45:00 OLD=kraken1.0_vs_INES NEW=kraken1.1_vs_INES
```

### Starting Fresh

If you need to completely reset and reprocess all data:

```bash
make ingest-reset  # ⚠️ This deletes existing parquet file and rebuilds from all snapshots
```

## Safety Features

### Server Safety
- **Read-only operations**: Never writes to server
- **Stability checks**: Waits for file writes to complete
- **Gentle polling**: 60+ minute intervals
- **Connection resilience**: Resumable rsync transfers

### Data Safety  
- **Atomic writes**: Temp file → rename (never corrupts existing data)
- **Append-only snapshots**: Never deletes historical data
- **Lock files**: Prevents overlapping sync operations
- **Backup mirroring**: Automatic iCloud copies

### Analysis Safety
- **Column existence checks**: Metrics only compute if required columns exist
- **Error handling**: Missing columns log warnings but don't break notebooks
- **Schema flexibility**: Multiple schema versions coexist in same dataset

## Troubleshooting

### No snapshots created
```bash
# Check sync logs
make logs

# Test connection manually
ssh username@server.example.com

# Check configuration  
cat configs/sources.yaml
```

### Missing curated data
```bash
# Check if snapshots exist
ls data/raw/cloud-11/snapshots/

# Run ingestion manually with debug
python3 -m pdb scripts/ingest.py
```

### Scheduler not working
```bash
# Check scheduler status
make scheduler-status

# Check logs
tail -f logs/scheduler.out.log
tail -f logs/scheduler.err.log

# Restart scheduler
make stop-scheduler && make start-scheduler
```

### Notebooks show "missing columns"
This is normal when the server schema changes. The notebook will:
- Skip metrics that need missing columns
- Show warnings for missing required columns  
- Continue working with available data

## Development

### Adding New Metrics
1. Edit `scripts/ingest.py` → `compute_derived_metrics()` 
2. Add column existence checks in notebooks
3. Test with different schema versions

### New Analysis Notebooks
1. Copy `notebooks/01_overview.ipynb` as template
2. Always check column existence before computing metrics:
   ```python
   if 'my_column' in df.columns:
       # compute metric
   else:
       print("my_column not available")
   ```

### Schema Changes
1. Update `schema_version` in `configs/sources.yaml`
2. Add new expected columns to `required_columns` list  
3. Test notebooks with both old and new data

## Data Retention

- **Raw snapshots**: Never deleted (append-only during thesis)
- **Curated Parquet**: Rebuilt on each ingestion (safe to delete)
- **iCloud backups**: Manual cleanup as needed
- **Logs**: Rotated/cleaned with `make clean`

## Performance

Typical performance on modern hardware:
- **Sync**: ~30-60 seconds for 10MB CSV  
- **Ingestion**: ~5-15 seconds for 100K rows
- **Analysis**: ~1-3 seconds to load curated Parquet

## License

MIT License - see LICENSE file