# F1 Telemetry Kalman Filter

## Quickstart: Generating the Dataset Locally

To keep the repository lightweight, we do not commit the raw Silverstone 2026 telemetry dataset (`.parquet`) to GitHub. Instead, use the included launcher to build the exact same DataFrame locally.

### Prerequisites
Make sure **Python 3.10+** is installed on your machine.

### 1. Build the Dataset

**Windows:**
1. Open the repository folder in File Explorer.
2. Double-click **`get_data.cmd`**.
3. A terminal window will open, install the required packages into a local `.venv` folder, download the 2026 Silverstone Grand Prix telemetry, and save the Parquet file.

**macOS / Linux:**
1. Open Terminal in the repository root folder and run:
   ```bash
   ./get_data.cmd