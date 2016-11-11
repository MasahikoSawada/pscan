# pscan
Test tool for parallel heap scan of PostgreSQL.

pscan has two types of parallel scanning method, **Tuple Scan(p_tuplescan)** and **Block Range Scan(p_brangescan)**.

## Tuple Scan
This scanning method uses parallel heap scan provided heapam.c which is used by Parallel sequential scan introduced in 9.6.
That is, p_tuplescan launchs worker that fetch tuple one by one, which means some parallel worker could access tuples on same page,
means that disk head moves sequentially.

## Block Range Scan
In this scanning method, each parallel worker has particular consecutive block range of target table.
Since different blocks located at faraway place could be accessed at the same time the disk head moves radomly,
but it might be more faster in case where all data are located on buffer.

# GUC parameter

`pscan.n_workers` specifies the number of parallel workers.

# Functions
## p_tuplescan(regclass)

Scan given table using tuple scanning method described above with parallel workers specified by `pscan.n_workers'.

## p_brangescan(regclass)

Scan given table using block range scanning method described above with parallel workers specified by second argument.

## p_tuplescan(regclass, int)

Scan given table using tuple scanning method described above with parallel workers specified by `pscan.n_workers'.

## p_brangescan(regclass, int)

Scan given table using block range scanning method described above with parallel workers specified by second argument.

# Compare with Parallel Seq Scan in PostgreSQL 9.6.

To compare with Parallel SeqScan in PostgreSQL 9.6, Both p_tuplescan() and p_brangescan() do and don't
following procedure.

## Don't
- Visibility check
  - They're assumed that table doesn't have garbage and is all visible.
- Filtering
  - They fetch all tuples.

## Do
- Read page
  - Allocate buffer and read all tuples on the page, and then release buffer.
- Read tuple
  - Fetch tuple one by one.
