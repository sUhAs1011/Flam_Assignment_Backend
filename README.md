# 🧵 QueueCTL — Background Job Queue CLI

QueueCTL is a lightweight, CLI-based background job processing system inspired by systems like Sidekiq/Celery. It supports job queuing, workers, retries with exponential backoff, Dead Letter Queue (DLQ), and persistent storage — all in a single-file Python implementation.

---

## 🚀 Setup Instructions

### ✅ Requirements

* Python 3.10+
* Ubuntu / WSL2 / Linux Terminal (fork + Unix signals)
* SQLite (auto-installed with Python)

### ✅ Installation & Run

```bash
git clone https://github.com/sUhAs1011/Flam_Assignment_Backend.git
cd queuectl
chmod +x queuectl.py
```

### ✅ Verify installation

```bash
python3 queuectl.py -h
```

---

## 💻 Usage Examples

### ▶ Start Workers

```bash
python3 queuectl.py worker start --count 2
```
<img width="804" height="103" alt="image" src="https://github.com/user-attachments/assets/f3e87805-c34f-4fc4-a489-eee5f0847c25" />

### ▶ Enqueue Jobs

Success job:

```bash
python3 queuectl.py enqueue --json '{"id":"job1","command":"echo Hello World"}'
```
<img width="1114" height="106" alt="image" src="https://github.com/user-attachments/assets/39915e7d-e34a-4210-a751-5c1e1dd772d9" />

Failing job (for retry + DLQ demo):

```bash
python3 queuectl.py enqueue --json '{"id":"fail1","command":"invalidcmd"}'
```
<img width="1075" height="102" alt="image" src="https://github.com/user-attachments/assets/c42a016f-44ad-4ae6-80e2-c3b081aa1545" />

### ▶ Check Status

```bash
python3 queuectl.py status
```
<img width="688" height="225" alt="image" src="https://github.com/user-attachments/assets/4b8d5fbc-fa85-4436-a803-a426bab835e8" />

### ▶ List Jobs

Pending:

```bash
python3 queuectl.py list --state pending
```
<img width="845" height="302" alt="image" src="https://github.com/user-attachments/assets/b720b2e3-08d2-46ec-a301-c60c3f22b424" />

Completed:

```bash
python3 queuectl.py list --state completed
```
<img width="906" height="347" alt="image" src="https://github.com/user-attachments/assets/bff90091-b1c9-42db-9b78-aaf34441e0f1" />

### ▶ Dead Letter Queue

View:

```bash
python3 queuectl.py dlq list
```
<img width="734" height="245" alt="image" src="https://github.com/user-attachments/assets/958d9b96-d89c-47d3-ad70-8aeed68067e5" />

Retry:

```bash
python3 queuectl.py dlq retry fail1
```
<img width="768" height="93" alt="image" src="https://github.com/user-attachments/assets/3edccc5a-e36e-47e3-a39a-2b0718c89c58" />


### ▶ Config Management

Show all:

```bash
python3 queuectl.py config get
```
<img width="744" height="172" alt="image" src="https://github.com/user-attachments/assets/89283243-b8f1-4f5f-9da3-debd50681c37" />

Update config:

```bash
python3 queuectl.py config set max_retries 5
python3 queuectl.py config set backoff_base 3
```
<img width="821" height="99" alt="image" src="https://github.com/user-attachments/assets/1f09fde6-3786-4387-b6bf-98f41d7aa1c5" />
<img width="863" height="111" alt="image" src="https://github.com/user-attachments/assets/82c39ce5-5dfd-44f1-bac3-1242129cb322" />

### ▶ Stop Workers Gracefully

```bash
python3 queuectl.py worker stop
```
<img width="795" height="173" alt="image" src="https://github.com/user-attachments/assets/9e944b60-f74e-4d94-a7a3-9e1c3619d8c9" />

---

## 🧠 Architecture Overview

### 📦 Components

| Component    | Purpose                       |
| ------------ | ----------------------------- |
| SQLite DB    | Stores job queue, DLQ, config |
| Workers      | Executes jobs in background   |
| PID files    | Tracks worker processes       |
| Logs         | Each job logs stdout/stderr   |
| Config store | Tweak retry/backoff/timeouts  |

### 🔄 Job Lifecycle

| State      | Meaning                            |
| ---------- | ---------------------------------- |
| pending    | Waiting to run                     |
| processing | Picked by a worker                 |
| completed  | Execution succeeded                |
| failed     | Temporary failure, retry scheduled |
| dead       | Max retries exceeded → DLQ         |


### 🔐 Concurrency Control

* SQLite WAL mode
* `BEGIN IMMEDIATE` lock prevents double execution
* Graceful shutdown → finishes current job then terminates

---

## ⚙️ Assumptions & Trade-offs

| Design Decision           | Reason                                          |
| ------------------------- | ----------------------------------------------- |
| SQLite for persistence    | Lightweight, no external dependencies           |
| Forked workers            | True multi-process concurrency (WSL/Linux only) |
| Single file design        | Meets assignment simplicity requirement         |
| No external queue brokers | Keeps system self-contained                     |
| Backoff limited by config | Avoid infinite growth                           |

---

## 🧪 Testing Instructions

| Test Scenario          | Steps                                     |
| ---------------------- | ----------------------------------------- |
| ✅ Successful job       | enqueue → observe completion              |
| ✅ Retry logic          | enqueue invalid command → observe retries |
| ✅ DLQ                  | failing job → view in DLQ                 |
| ✅ DLQ retry            | `dlq retry <id>` → job requeues           |
| ✅ Multi-worker         | start 2 workers → enqueue multiple jobs   |
| ✅ Duplicate prevention | only one worker picks a job               |
| ✅ Persistence          | exit + re-run → jobs remain in DB         |
| ✅ Graceful shutdown    | `worker stop` → active job completes      |

---

## 🌟 Bonus Features Implemented

| Feature        | Description                               |
| -------------- | ----------------------------------------- |
| Job timeout    | Auto-kill long-running jobs               |
| Priority queue | Lower priority number = earlier execution |
| Scheduled jobs | `run_at` controls deferred execution      |
| Execution logs | `.queuectl/logs/<job_id>.log`             |

---

## 🎥 Demo Video

📎 https://drive.google.com/file/d/1e48AIhuVWHCWIjoZ6MxMHIgVf_9X5IOb/view?usp=sharing

---



