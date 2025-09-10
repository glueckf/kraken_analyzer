PY=python3

# Core workflow commands
.PHONY: install-deps analyze sync ingest status clean
# Experiment management commands  
.PHONY: experiment-status experiment-periods experiment-validate fix-labels fix-labels-quick
# Advanced management commands
.PHONY: backup-status schema-status timezone-info ingest-reset
# Scheduler commands
.PHONY: start-scheduler stop-scheduler scheduler-status logs

# === QUICK START ===

# Install required Python dependencies
install-deps:
	$(PY) -m pip install pandas pyarrow pyyaml python-dotenv

# One-click analysis: sync + ingest (recommended workflow)
analyze: sync ingest
	@echo "Data synced & curated. Open notebooks/01_overview.ipynb"
	@echo "Curated data: data/curated/experiments.parquet"

# === CORE OPERATIONS ===

# Sync data from server (pull only, safe)
sync:
	./scripts/sync_server.sh

# Process raw snapshots into curated parquet (incremental)
ingest:
	$(PY) scripts/ingest.py

# Show comprehensive status
status:
	@echo "=== Kraken Analyzer Status ==="
	@printf "Raw snapshots: %s files\n" "$$(ls -1 data/raw/cloud-11/snapshots/*.csv 2>/dev/null | wc -l | tr -d ' ' || echo '0')"
	@printf "Latest snapshot: %s\n" "$$(ls -1t data/raw/cloud-11/snapshots/*.csv 2>/dev/null | head -n 1 | xargs basename || echo 'none')"
	@printf "Curated data: %s\n" "$$(test -f data/curated/experiments.parquet && echo 'available' || echo 'missing')"
	@printf "Current experiment: %s\n" "$$(grep EXPERIMENT_LABEL .env 2>/dev/null | cut -d'=' -f2 || echo 'unknown')"

# === EXPERIMENT MANAGEMENT ===

# Show current experiment data status
experiment-status:
	@echo "=== Experiment Data Status ==="
	@if [ -f data/curated/experiments.parquet ]; then \
		$(PY) -c "import pandas as pd; df = pd.read_parquet('data/curated/experiments.parquet'); print('Total rows:', len(df)); print('\\nExperiment breakdown:'); print(df['experiment_label'].value_counts().to_string()); print('\\nSnapshot files:'); print(df['snapshot_file'].value_counts().to_string())"; \
	else \
		echo "No curated data found. Run 'make analyze' first."; \
	fi

# Show experiment periods configuration
experiment-periods:
	@echo "=== Experiment Periods Configuration ==="
	@if [ -f data/experiment_periods.json ]; then \
		$(PY) scripts/experiment_manager.py list; \
	else \
		echo "No experiment periods file found. System will use environment variables for labeling."; \
	fi

# Validate experiment periods
experiment-validate:
	@echo "=== Validating Experiment Periods ==="
	@if [ -f data/experiment_periods.json ]; then \
		$(PY) scripts/experiment_manager.py validate; \
	else \
		echo "No experiment periods file to validate."; \
	fi

# Enhanced experiment label fixing
fix-labels:
	@echo "=== Enhanced Experiment Label Fixing ==="
	@echo "Available strategies:"
	@echo "  1. Quick fix (most common): make fix-labels-quick CUTOFF=YYYY-MM-DDTHH:MM:SS OLD=label1 NEW=label2"
	@echo "  2. Advanced fix: Uses experiment periods file for precise control"
	@echo ""
	@echo "For the specific kraken1.0 vs kraken1.1 fix:"
	@echo "  make fix-labels-quick CUTOFF=2025-09-10T08:00:00 OLD=kraken1.0_vs_INES NEW=kraken1.1_vs_INES"

# Quick label fix (most common use case)
fix-labels-quick:
	@if [ -z "$(CUTOFF)" ] || [ -z "$(OLD)" ] || [ -z "$(NEW)" ]; then \
		echo "Usage: make fix-labels-quick CUTOFF=YYYY-MM-DDTHH:MM:SS OLD=label1 NEW=label2"; \
		echo "Example: make fix-labels-quick CUTOFF=2025-09-10T08:00:00 OLD=kraken1.0_vs_INES NEW=kraken1.1_vs_INES"; \
		exit 1; \
	fi
	@echo "Dry run first (showing proposed changes)..."
	@$(PY) scripts/fix_experiment_labels.py cutoff --cutoff-time "$(CUTOFF)" --old-label "$(OLD)" --new-label "$(NEW)" --dry-run
	@echo ""
	@read -p "Apply these changes? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	@$(PY) scripts/fix_experiment_labels.py cutoff --cutoff-time "$(CUTOFF)" --old-label "$(OLD)" --new-label "$(NEW)"

# === MAINTENANCE & DIAGNOSTICS ===

# Clean up temporary files and logs (keeps all data)
clean:
	@echo "Cleaning temporary files..."
	@rm -f logs/*.log logs/.sync.lock
	@rm -f data/raw/cloud-11/snapshots/.run_results.*.tmp.csv
	@echo "Cleanup complete (data preserved)"

# Reset and reprocess all snapshots from scratch (dangerous)
ingest-reset:
	@echo "WARNING: This will reset all experiment data and reprocess from scratch"
	@echo "Current parquet file will be deleted and rebuilt from all snapshots"
	@read -p "Continue? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	@$(PY) scripts/ingest.py --reset

# Show backup folder organization  
backup-status:
	@echo "=== Backup Status ==="
	@if [ -n "$(ICLOUD_DEST)" ] && [ -d "$$(dirname $(ICLOUD_DEST) 2>/dev/null || echo /nonexistent)" ]; then \
		echo "iCloud backup: enabled"; \
		echo "Backup path: $(ICLOUD_DEST)"; \
	else \
		echo "iCloud backup: not configured"; \
	fi

# Show schema and file status  
schema-status:
	@echo "=== Schema & Files Status ==="
	@echo "Schema version: $(shell grep SCHEMA_VERSION .env 2>/dev/null | cut -d'=' -f2 || echo 'v1')"
	@echo "Parquet files:"
	@ls -lah data/curated/experiments*.parquet 2>/dev/null || echo "  No parquet files found"

# Show timezone information
timezone-info:
	@$(PY) scripts/timezone_utils.py now

# === AUTOMATION (OPTIONAL) ===

# Start automated scheduler (every 60 minutes)
start-scheduler:
	@echo "Starting automated scheduler..."
	@launchctl load -w "$(PWD)/launchd/com.kraken.analyzer.sync.plist" && echo "Scheduler started (every 60 min)" || echo "Failed to start scheduler"

# Stop automated scheduler  
stop-scheduler:
	@launchctl unload "$(PWD)/launchd/com.kraken.analyzer.sync.plist" && echo "Scheduler stopped" || echo "Failed to stop scheduler"

# Check scheduler status
scheduler-status:
	@printf "Scheduler: %s\n" "$$(launchctl list | grep com.kraken.analyzer.sync >/dev/null && echo 'running' || echo 'stopped')"

# Show recent logs
logs:
	@echo "=== Recent Sync Logs ==="
	@ls -1t logs/rsync_*.log 2>/dev/null | head -3 | xargs tail -n 10 || echo "No logs found"