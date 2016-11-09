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
## p_tuplescan(table)

Scan given table using tuple scanning method described above.

## p_brangescan(table)

Scan given table using block range scanning method described above.

