# From source identity to sink revision

```mermaid
sequenceDiagram
    participant S as Source replay
    participant P as Processor
    participant DB as SQLite WAL
    participant O as Offline oracle
    S->>P: Validated partition offset and event
    P->>DB: BEGIN IMMEDIATE
    P->>DB: Check identity and configuration
    P->>DB: Update partition progress and windows
    P->>DB: Retraction upsert finalization and checkpoint
    P->>DB: COMMIT
    P-->>S: Compact receipt
    S->>P: Replay after crash or lost acknowledgement
    P->>DB: Check identical durable source identity
    P-->>S: Duplicate with no repeated effect
    O->>O: Recompute accepted contributions from history
    DB-->>O: Consistent sink snapshot for comparison
```

`processor.py` owns validation, event-time policy, transaction state and sink updates. `io.py` bounds local JSON ingestion. `oracle.py` has independent history-based aggregation and a separate changelog consumer. `experiments.py` injects crashes, reopens real database files and checks replay against those references. The CLI supports fixed demonstrations/evaluation and actual persistent JSONL consumption.

See [watermark-contract.md](watermark-contract.md) for mathematics, transaction boundaries, capacities and failure semantics; [window-histories.md](window-histories.md) for executed evidence.
