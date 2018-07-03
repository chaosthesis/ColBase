# CS165 Project Report

#### Hongchao Wang | Fall 2017

## Overview

This project is an implementation of a basic column-store data system following the course handout as a guideline. It consists of five milestones, each of which focuses on a particular set of problems:

- **Milestone 1:** create, load, persists; select, fetch, aggregate, print
- **Milestone 2:** shared scan
- **Milestone 3:** sorted index, btree index; clustered, unclustered
- **Milestone 4:** nested-loop join, hash join
- **Milestone 5:** delete, update

With every module in place, we have a functional database to experiment different design choices and tunning parameters.

## Milestone 1: Basic Column-store

### 1.1 Introduction

The goal is to design and implement the basic functionality of a column-store with the ability to run single-table queries. The first part of this milestone is about the storage and organization of relational data in the form of columns, one per attribute. This requires the creation and persistence of a database catalog containing metadata about the tables in your database as well as the creation and maintenance of files to store the data contained in these tables. The next step is to implement a series of scan-based operators on top of the columns to support queries with selection, projection, aggregation and math operators. Then using these operators, we can synthesize single-table query plans such as SELECT max(A) FROM R where B< 20 and C > 30 that work with a bulk processing data flow.

### 1.2 Implementation Highlights

#### 1.2.1 Database Catalog and Variable Pool

- **Problem framing:** 1. How do we design a database catalog to keep track of tables, columns, and indices? 2. How do we maintain a variable pool so we can locate intermediate results by name during client-server communications?
- **High-level solution:** For database catalog, a set of hierarchical data structures are specified for database, table, and column respectively -- each contains a pointer to its next-level data structure or payload. For variable pool, the system maintains another data structure in memory to keep the name and payload of an intermediate result, and it frees all the data once the corresponding client exits.
- **Deeper details:** For database catalog, the structure hierarchy looks something like this: `Db{meta_data, Tables[tbl1, tbl2, ...]}`; `Table{meta_data, Columns[col1, col2, ...]`; `Column{meta_data, payload[val1, val2, ...], index}`; `Index{type, payload}`. These objects can be accessed through a top-level global variable `current_db`, which is of structure `Db`. For variable pool, the structure hierarchy looks something like this: `ClientContext{meta_data, Handles[handle1, handle2, ...]}`; `Handle{name, payload}`. There is a local `client_context` instance for each connected client. When the system is running, all the data structures reside in memory and wait for queries.

#### 1.2.2 Data Synchronization

- **Problem framing:** Data synchronization problem has two parts: how to persist data on disk and how to load data back into memory.
- **High-level solution:** I used directories to map the hierarchy of every data object in the database catalog, and used file names to denote instance names. For smaller meta-data files, the system fully loads/syncs them on startup/shutdown; but for larger payload files, the system maps them to memory first, and read/write them only on demand.
- **Deeper details:** For each database objects, meta-data and payload are stored in the same directory but not in the same file. In each directory, there is a meta-data file containing descriptive information such as name, length, capacity, plus the names of next-level data objects; also, there is a payload file keeping the actual data or just a subdirectory containing everything for its next-level database objects. Although we know the data can easily fit in the main memory, my implementation also takes advantage of the `mmap` library to map the persisted payload data on disk to memory addresses. This speeds up data system boot up time and extends its ability to handle large datasets.
   
#### 1.2.3 Client-server Communication and Bulk Loading

- **Problem framing:** The socket has fixed capacity of sending messages, which is hard-coded and varies across Unix platforms. The client-server communication limitation emerges in scenarios involving bigger-than-capacity message exchange, such as: 1. client send a big local file to server using `load` operator, and 2. server responds the client query with a large chunk of string.
- **High-level solution:** In our testbed environment, the `send()` function is a blocking method while the `recv()` function is not. It indicates that the `send()` function will not return until all messages have been sent (or connection is closed), whereas the `recv()` function returns immediately once it has received the message of expected length or the maximum length that socket can handle. There has to be some coordination between `send()` and `recv()`. Then, with the ability to exchange large data, the bulk loading becomes possible to facilitate more efficient data loading.
- **Deeper details:** 1. To make `send()` work for large messages, I changed the behavior on the receiving end so it keeps receiving messages and counting received characters until it gets the whole message or `send()` returns on the other side. Note that a similar mechanism is used to implement the `print` operator, as the printing handler has no knowledge of result size while formatting result tuples to a string, and it has to keep dynamically writing and resizing write buffer. 2. In terms of loading, as opposed to parsing data file and inserting data records line by line, my implementation aggregate the source file line by line and convert them into columns first, then use the above `send()` method to bulk load to the server.

### 1.3 Experiments

- Direct read vs mmap
    |         |  Direct read  | mmap |
    | ------- | --------- | ------- |
    | Test 11 |  34793 us | 10220 us |
    | Test 20 |  63978 us | 38889 us |
    | Test 26 |  104475 us | 68425 us |

    It shows that the system saves significant time by not loading everything into main memory on startup.

## Milestone 2: Fast Scans: Scan Sharing & Multi-cores

### 2.1 Introduction

The goal is to add support for scan sharing to minimize data movement and making scans use multiple cores to fully utilize parallelization opportunities of modern CPUs. The system should be able to run N>>1 queries in parallel by reading data only once for the different queries’ select operators. The end goal is to be able to run simple single-table queries but use the new select operators that are capable of scan sharing. The client can declare a batch of queries to execute and then tell the server to execute them all at once. The server then must coordinate with the client when it is done with this batch.

### 2.2 Implementation Highlights

#### 2.2.1 Scan sharing

- **Problem framing:** In light of modern multicore CPU's ability to multithread and share cache space, how can we improve the performance of scan operation in such architecture?
- **High-level solution:** Client signals server both the arrival and stop of batch queries. Server records all the received batch queries in the corresponding client context, and executes them only when signaled to do so. When executing, the server calls a thread pool module to schedule all the query tasks, so that multiple (e.g. the number of cores) tasks are running concurrently and ideally share some overlap data in L1 cache.
- **Deeper details:** To achieve multithreading, I used a `pthread_mutex_lock` in the `<pthread.h>` library to implement a minimal thread pool. Two important static variables are used to control the thread behavior: a `size_t jobs` which denotes the number of select operators left, and a `pthread_mutex_t lock` to ensure the `jobs` value stays correct. Then, an array of size `PROC_NUM` is used to simulate a thread pool. For each entry of this array, a thread is created by calling `pthread_create` and execute `select` operators continuously until `jobs` reaches 0. The share scan function returns when every thread in this array has been finished.

### 3. Experiments

- Scan vs Shared scan, batch size = 20

    | Core #  |  Scan      | Shared scan |
    | ------- | ---------- | ----------- |
    |   8     |  165927 us | 137582 us   |
    |   16    |  175195 us | 115594 us   |
    |   32    |  175195 us | 120870 us   |

    It indicates that shared scan is faster due to better cache usage. However, the level of parallelism does not grow linearly with the number of cores; it is convergent.

## Milestone 3:  Indexing

### 3.1 Introduction

The goal is to add indexing support to boost select operators. The system should support memory-optimized B-tree indexes and sorted columns in both clustered and unclustered form. In the first case, all columns of a table follow the sort order of a leading column. The end goal is to be able to run single-table queries (as in the first milestone) but use the new index-based select operators instead. The B-tree should be tuned for main-memory, i.e., to minimize cache misses by properly tuning design parameters such as the fan-out of the tree based on the underlying hardware as well as any other design parameter that may affect performance.

### 3.2 Implementation Highlights

#### 3.2.1 A Minimal B-Tree

- **Problem framing:** How to design and implement a `B-tree` that works reasonably for the workload of this project?
- **High-level solution:** A `B-tree` is a collection of `B-tree` nodes. Each `B-tree` node contains some meta information to record its type (`internal node` or `leaf`), length, etc. Each node also has an array of values; and depends on its type, it has an array of `children` (if it is `internal node`) or `indices` (if it is `leaf`). For `leaf` nodes, there is also a `next` pointer which points to the next `leaf` node. With this data structure, I implemented a set of functions to build, modify, search, and print the `B-tree`.
- **Deeper details:** Following the recommendation in the course, my `B-tree` implementation has the following assumptions:
    - No redistribution, only splits
    - No merges, only merge on empty
    - No parent pointers
    - No prev pointers at leaves; no prev/next pointers at internal nodes
    
    With that, the follwing functions are exposed to the system's `index` module to support all needed indexing functions:
    - `BTreeNode* build_btree(int* vals, size_t* idxs, size_t length)`
    - `BTreeNode* btree_search(BTreeNode* node, int val)`
    - `void btree_insert(BTreeNode** node, int val, size_t val_pos, bool clustered)`

    There are a number of small functions (e.g. `btree_node_move`, `split_btree_child`, `link_btree_node`) in `bree.c` file to support the above functions. The detailed design is hard to summarise in a few sentences, but it is closely modeled after CS165 class examples, and to some extent it is quite self-explanatory since my implementation is modular.

#### 3.2.2 Handle Clustered/Unclustered Index

- **Problem framing:** How to handle (create, update, search) indices under different database schemas (clustered/unclustered, btree/sorted)?
- **High-level solution:** We need to approach the different kinds of indices using a 2-phase workflow. The first phase is to identify the index's role in that table -- is it clustered or unclustered? This phase sometimes must come first because it influences our decision to initialize the column. The second phase is to identify the index data structure type -- is it a B-tree or a sorted array? This phase sometimes can come next because it only comes into play when scanning and updating data after the column has already been initialized. The order of the two phases depends on the specific operator.
- **Deeper details:** The following is the two-phase workflow for `load` and `select`. The workflows for other operators are similar in spirit.

    |        |  Phase 1  | Phase 2 |
    | ------ | --------- | ------- |
    |  Load  | if `clustered`: prepare to propagate order | if `sorted`: add to base data -> sort -> output order |
    |        |                                            | if `btree`: add to btree -> output order |
    |        | if `unclustered`: go to phase 2            | if `sorted`: add to index payload -> sort data&position |
    |        |                                            | if `btree`: add to btree -> update order |
    | Select | if `sorted`:  go to phase 2                | if `clustered`: select from base data |
    |        |                                            | if `unclustered`: select from index payload |
    |        | if `btree`: select from btree -> stop      | nothing |

### 3.3 Experiments

- Scan vs Sorted vs B-tree (10% selectivity)

    |  Scan   |  Sorted  | B-tree  |
    | ------- | -------- | ------- |
    | 6582 us |  3366 us | 3023 us |
    | 7265 us |  3870 us | 3425 us |
    | 8362 us |  4468 us | 3973 us |
    
    In a typical range query, both sort index and B-tree index performs much better than a full scan. Relatively, B-tree index performs better than sorted index because even fewer pages were touched.

- Scan vs B-tree (range query)

    |  Selectivity |  Scan / Index  |
    | ------------ | -------------- |
    |     0%       |    2.74        |
    |     10%      |    2.58        |
    |     50%      |    2.24        |
    |     70%      |    1.34        |
    |     100%     |    0.88        |
    
    Choosing index is not always the optimal option because when the selectivity is high, the calling of oracles becomes an overhead which also doesn't help reduce data movement cost. As we can see in this experiment, it makes more and more sense to choose scan rather than index as selectivity increases.

## Milestone 4: Joins

### 4.1 Introduction

The goal is to add join support to the system. It should support cache-conscious hash joins and nested-loop join that utilize all cores of the underlying hardware.

### 4.2 Implementation Highlights

#### 4.2.1 Linear Hashtable

- **Problem framing:** When using hash join, we build a hash table on R initially and discard it after the join operation finishes, thus the cost of building a hash table is essential for performance. So which hash table implementation can we choose so that it can build fast?
- **High-level solution:** I chose to implement a dynamically updateable index structure called linear hashing, which grows or shrinks one bucket at a time. Compared with linked-list hash table, linear hashtable does not suffer from the expensive resizing operation which needs to rebuild the whole hash table during the building process. Compared with extendible hash table, linear hash table does not use a bucket directory.
- **Deeper details:** The initial hash table is set up with a number of buckets. It keeps track of the current `round` number (initialized to 0), a `next_split` number denoting which bucket to split next. As we keep adding <key, value> pairs to the hash table, when a bucket has reached maximum capacity, the hash table: 1. append a bucket at the end of current bucket and insert the data; 2. create a new bucket at the end of buckets list; 3. redistribute items in the bucket at location `next_split`; 4. increase `next_split` by 1; if it equals some power of 2, increase `round` by 1 and reset `next_split` to 0. Also, linear hashtable maintains two hash functions at any given time and uses one of them based on current bucket number.

#### 4.2.2 Multi-core Hash Join

- **Problem framing:** How to implement a main memory hash join function for multi-core CPUs?
- **High-level solution:** The goal is to make the whole process parallel as much as possible. The intuition is that it is doable in both partition phase and probe phase. In partition phase, we can assign each partition to a CPU core to build its respective hash table; in probe phase, again, we assign each partition to a CPU core to do the probing, and merge the results.
- **Deeper details:** I tried to implement a partition function, but the overall join performance was actually slower. I believe the reason is that the partition stage took much longer than the join stage, which might indicate the partition hash join is not suitable for our workload, and the paper *"Design and Evaluation of Main Memory Hash Join Algorithms for Multi-core CPUs"* has a similar theory. For this reason, I skipped the partition phase and focused on the probe phase. The implementation is similar to shared scan. I maintained a `pthread_t` array of size `PROC_NUM` as thread pool, each thread applies a key to the hash table and gets back a result tuple; a `pthread_mutex_t` lock is here to make sure the results are written at the correct offset of the shared result tuple. But with this implementation, the join performance was also slower because the frequent `pthread_mutex_lock` and `pthread_mutex_unlock` calls took a lot of cycles. I am still looking for a better approach.

### 4.3 Experiments

- Hash Bucket Size

    | Bucket Slot |  Hash Join  |
    | ----------- | ----------- |
    |     5       |  175195 us  |
    |     50      |  256742 us  |
    |     100     |  503706 us  |

    In our use case, we probe the hash table which resides in main memory many times, so we should optimize it for L1 cache level. As we can see in this experiment, when a bucket has fewer slots, it's size fits in cache line better, which results in faster probing.

## Milestone 5: Updates

### 5.1 Introduction

The goal is to add update support. The system should support inserts, deletes, and updates on relational tables,  maintaining the correctness of the various indexes on the base data. Any changes to data should be persistent. An update requires changing each copy of the data. This means keeping metadata around in some form such that updates made on one copy of the data get propagated to the other copies of the data.

### 5.2 Implementation Highlights

#### 5.2.1 Updates and deletes

- **Problem framing:** How to update/delete data and make sure every copy/index stay consistent?
- **High-level solution:** 1. Since the `update` operation can be modeled as a `delete` followed by an already implemented `insert`, so I only focused on the `delete` operation. 2. Similiar to `insert`, my `delete` implementation used a 2-phase workflow.
- **Deeper details:** 
    |        |  Phase 1  | Phase 2 |
    | ------ | --------- | ------- |
    | Delete | delete column payload values at given positions | rebuild position indices in column payload and index |
