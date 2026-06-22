# OS Semester Project - Parallel CSV Data Processing Pipeline

## How to Run

**1. Make the script executable (one time only):**
```bash
chmod +x run.sh
```

**2. Run the pipeline:**
```bash
./run.sh -i data -o output -n 4 -q 100
```

| Flag | Description |
|------|-------------|
| `-i data` | Input directory containing CSV files |
| `-o output` | Output directory for report.txt and report.csv |
| `-n 4` | Number of worker threads |
| `-q 100` | Bounded queue size |

**3. Results will be in the `output/` folder:**
- `output/report.txt` — Human-readable summary
- `output/report.csv` — Machine-readable CSV

**4. To stop early, press:**
```bash
Ctrl+C
```