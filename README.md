# Real-Time Multithreaded Sensor Data Pipeline (C++ / POSIX)

A high-throughput, multi-process IoT telemetry processing pipeline built in C++ using POSIX systems-programming abstractions. The architecture is explicitly engineered to minimize synchronization overhead and eliminate lock contention during real-time data ingestion, aggregation, and anomaly detection.

## 🚀 Key Architectural Highlights

* **Multi-Process Orchestration:** Uses a centralized parent process (`Dispatcher`) to fork and execute decoupled modules (`Ingester`, `Processor`, `Reporter`) using `fork()` and `execvp()`, handling child processes cleanly via async `SIGCHLD` signal capture.
* **Lock-Free Local Aggregation:** Eliminates multi-thread bottlenecking (lock contention) by using Thread-Local Storage (TLS) hash maps (`std::unordered_map`) to aggregate readings locally before a single-lock batch commit to the global registry.
* **Low-Latency IPC:** Combines UNIX named pipes (FIFOs) for asynchronous raw stream delivery, POSIX Shared Memory mapped via `mmap()` for zero-copy inter-process calculations, and Named Cross-Process Semaphores for signal handoffs.
* **Bounded-Buffer Control:** Implements flow control through custom bounded queues synchronized by a double-semaphore design to introduce system backpressure and prevent memory exhaustion.
* **Robust Signal Handling:** Features custom terminal signal handling (`SIGINT`/`SIGTERM`) ensuring immediate, leak-free reclamation of IPC handles (`sem_unlink`, `shm_unlink`) and clean child termination.

## 🛠️ Tech Stack & Concepts
* **Language:** Modern C++ (C++17)
* **API Standards:** POSIX Systems Interfaces
* **Threading Engine:** POSIX Threads (`pthreads`), Mutexes, Semaphores
* **IPC Mechanics:** Shared Memory (`shm_open`, `mmap`), Named Pipes (FIFOs), Signals

## 📂 Project Structure
```text
├── data/                 # Input CSV directory (Ingester source)
├── output/               # Output directory (Reporter target)
├── logs/                 # Isolated subprocess run logs
├── src/                  # Source folder for pipeline modules
│   ├── ingester.cpp      # File explorer & batch streamer
│   ├── processor.cpp     # Multithreaded parser & statistical engine
│   └── reporter.cpp      # Shared memory reader & exporter
├── dispatcher.cpp        # Master system orchestrator
├── run.sh                # Automation script for build & test run
└── .gitignore            # Clean repository filter rules
```

## ⚡ Quick Start

1. **Prerequisites**

   Ensure you are running on a Linux/UNIX environment with `g++` and POSIX development libraries.

2. **Execution via Automation Script**

   Use the built-in compiler and execution helper script to launch the full pipeline immediately with configured parameters:

   ```bash
   # Make the run script executable
   chmod +x run.sh

   # Compile and run the pipeline
   ./run.sh -i ./data -o ./output -n 4 -q 100 -f /tmp/sensor_fifo -s /sensor_shm
   ```

---

## 📖 Full Documentation
For the full documentation and explanation of the project, visit the [Project Report](./report/).
