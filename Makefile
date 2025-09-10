PY=python3

.PHONY: sync ingest analyze status start-scheduler stop-scheduler install-deps ingest-reset fix-labels

# Install required Python dependencies
install-deps:
	$(PY) -m pip install pandas pyarrow pyyaml python-dotenv

# Sync data from server (pull only, safe)
sync:
	./scripts/sync_server.sh

# Process raw snapshots into curated parquet (incremental)
ingest:
	$(PY) scripts/ingest.py

# One-click analysis: sync + ingest
analyze: sync ingest
	@echo "Data synced & curated. Open notebooks/01_overview.ipynb"
	@echo "Curated data: data/curated/experiments.parquet"

# Show current status
status:
	@echo "=== Kraken Analyzer Status ==="
	@echo "Raw snapshots:" && (ls -1 data/raw/cloud-11/snapshots/*.csv 2>/dev/null | wc -l | tr -d ' ' || echo "0") && echo " files"
	@echo "Latest snapshot:" && (ls -1t data/raw/cloud-11/snapshots/*.csv 2>/dev/null | head -n 1 | xargs basename || echo "none")
	@echo "Latest symlink:" && (test -L data/raw/cloud-11/latest.csv && echo "exists" || echo "missing")
	@echo "Curated parquet:" && (test -f data/curated/experiments.parquet && echo "exists" || echo "missing")
	@echo "Config experiment:" && (awk '/experiment_label:/{print $$2}' configs/sources.yaml | tr -d '"' || echo "unknown")

# Start automated scheduler (every 60 minutes)
start-scheduler:
	@echo "Loading launchd job for automated analysis (every 60 min)..."
	@launchctl load -w "$(PWD)/launchd/com.kraken.analyzer.sync.plist" && echo "Scheduler started" || echo "Failed to start scheduler"

# Stop automated scheduler  
stop-scheduler:
	@echo "Unloading launchd job..."
	@launchctl unload "$(PWD)/launchd/com.kraken.analyzer.sync.plist" && echo "Scheduler stopped" || echo "Failed to stop scheduler"

# Check if scheduler is running
scheduler-status:
	@echo "Scheduler status:"
	@launchctl list | grep com.kraken.analyzer.sync || echo "Not running"

# Clean up temporary files and logs (but keep snapshots and curated data)
clean:
	rm -f logs/*.log
	rm -f logs/.sync.lock
	rm -f data/raw/cloud-11/snapshots/.run_results.*.tmp.csv

# Development: run ingest only (assumes snapshots exist)
dev-ingest: ingest
	@echo "Development ingest complete"

# Show recent logs
logs:
	@echo "=== Recent sync logs ==="
	@ls -1t logs/rsync_*.log 2>/dev/null | head -3 | xargs tail -n 10 || echo "No logs found"

# === EXPERIMENT MANAGEMENT ===

# Reset and reprocess all snapshots from scratch (use when starting fresh)
ingest-reset:
	@echo "⚠️  WARNING: This will reset all experiment data and reprocess from scratch"
	@echo "Current parquet file will be deleted and rebuilt from all snapshots"
	@read -p "Continue? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	$(PY) scripts/ingest.py --reset

# Fix experiment labels when data was incorrectly labeled
fix-labels:
	@echo "This tool helps fix experiment labels when snapshots were mislabeled"
	@echo "You need to provide:"
	@echo "  - cutoff-time: Timestamp to separate experiments (YYYY-MM-DDTHH:MM:SS)"  
	@echo "  - old-label: Label for snapshots before cutoff"
	@echo "  - new-label: Label for snapshots after cutoff"
	@echo ""
	@echo "Example: make fix-labels CUTOFF=2025-09-10T06:45:00 OLD=kraken1.0_vs_INES NEW=kraken1.1_vs_INES"
	@echo ""
	@if [ -z "$(CUTOFF)" ] || [ -z "$(OLD)" ] || [ -z "$(NEW)" ]; then \
		echo "Usage: make fix-labels CUTOFF=<time> OLD=<label> NEW=<label>"; \
		exit 1; \
	fi
	@echo "Dry run first..."
	$(PY) scripts/fix_experiment_labels.py --cutoff-time "$(CUTOFF)" --old-label "$(OLD)" --new-label "$(NEW)" --dry-run
	@read -p "Apply these changes? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	$(PY) scripts/fix_experiment_labels.py --cutoff-time "$(CUTOFF)" --old-label "$(OLD)" --new-label "$(NEW)"

# Show current experiment data status
experiment-status:
	@echo "=== Experiment Data Status ==="
	@if [ -f data/curated/experiments.parquet ]; then \
		$(PY) -c "import pandas as pd; df = pd.read_parquet('data/curated/experiments.parquet'); print('Total rows:', len(df)); print('\\nExperiment breakdown:'); print(df['experiment_label'].value_counts().to_string()); print('\\nSnapshot files:'); print(df['snapshot_file'].value_counts().to_string())"; \
	else \
		echo "No curated data found. Run 'make analyze' first."; \
	fi