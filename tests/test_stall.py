import json
import numpy as np


ftdc_metrics_keys = (
    "start",
    # When starting with @ apply a rated differential
    # WT tickets
    "serverStatus.wiredTiger.concurrentTransactions.read.out",
    "serverStatus.wiredTiger.concurrentTransactions.write.out",
    "serverStatus.wiredTiger.concurrentTransactions.read.totalTickets",
    "serverStatus.wiredTiger.concurrentTransactions.write.totalTickets",
    # application threads
    "@serverStatus.wiredTiger.lock.checkpoint lock application thread wait time (usecs)",
    "@serverStatus.wiredTiger.lock.dhandle lock application thread time waiting (usecs)",
    "@serverStatus.wiredTiger.lock.durable timestamp queue lock application thread time waiting (usecs)",
    "@serverStatus.wiredTiger.lock.metadata lock application thread wait time (usecs)",
    "@serverStatus.wiredTiger.lock.read timestamp queue lock application thread time waiting (usecs)",
    "@serverStatus.wiredTiger.lock.schema lock application thread wait time (usecs)",
    "@serverStatus.wiredTiger.lock.table lock application thread time waiting for the table lock (usecs)",
    "@serverStatus.wiredTiger.lock.txn global lock application thread time waiting (usecs)",
    "@serverStatus.wiredTiger.cache.cache overflow cursor application thread wait time (usecs)",
    # global WT lock acquistions
    "@serverStatus.wiredTiger.lock.txn global read lock acquisitions",
    "@serverStatus.wiredTiger.connection.pthread mutex shared lock read-lock calls",
    "@serverStatus.wiredTiger.connection.pthread mutex shared lock write-lock calls",
    "@serverStatus.wiredTiger.connection.pthread mutex condition wait calls",
    "@serverStatus.wiredTiger.transaction.transaction range of IDs currently pinned",
    "@serverStatus.wiredTiger.transaction.transaction range of IDs currently pinned by a checkpoint",

    # internal threads
    "@serverStatus.wiredTiger.lock.checkpoint lock internal thread wait time (usecs)",
    "@serverStatus.wiredTiger.lock.dhandle lock internal thread time waiting (usecs)",
    "@serverStatus.wiredTiger.lock.metadata lock internal thread wait time (usecs)",
    "@serverStatus.wiredTiger.lock.durable timestamp queue lock internal thread time waiting (usecs)",
    "@serverStatus.wiredTiger.lock.read timestamp queue lock internal thread time waiting (usecs)",
    "@serverStatus.wiredTiger.lock.schema lock internal thread wait time (usecs)",
    "@serverStatus.wiredTiger.lock.table lock internal thread time waiting for the table lock (usecs)",
    "@serverStatus.wiredTiger.lock.txn global lock internal thread time waiting (usecs)",
    # capacities? learning how these work as a function of available CPU time (what domain?)...
    "@serverStatus.wiredTiger.capacity.time waiting due to total capacity (usecs)",
    "@serverStatus.wiredTiger.capacity.time waiting during checkpoint (usecs)",
    "@serverStatus.wiredTiger.capacity.time waiting during eviction (usecs)",
    "@serverStatus.wiredTiger.capacity.time waiting during logging (usecs)",
    "@serverStatus.wiredTiger.capacity.time waiting during read (usecs)",

    # cache, full-ness & pressure - unrated
    "serverStatus.wiredTiger.cache.tracked dirty bytes in the cache",
    "serverStatus.wiredTiger.cache.bytes currently in the cache",
    "serverStatus.wiredTiger.cache.bytes dirty in the cache cumulative",
    "serverStatus.wiredTiger.cache.bytes not belonging to page images in the cache",
    "serverStatus.wiredTiger.cache.bytes belonging to the cache overflow table in the cache",

    # cache, storage demand & pressure
    "@serverStatus.wiredTiger.cache.bytes read into cache",
    "@serverStatus.wiredTiger.cache.bytes written from cache",
    "@serverStatus.wiredTiger.connection.total read I/Os",
    "@serverStatus.wiredTiger.connection.total write I/Os",
    "@serverStatus.wiredTiger.block-manager.bytes read",
    "@serverStatus.wiredTiger.block-manager.bytes written",

    # checkpoint pressure
    "serverStatus.wiredTiger.transaction.transaction checkpoint currently running",  # unrated
    "serverStatus.wiredTiger.connection.files currently open",  # unrated
    "@serverStatus.wiredTiger.cache.eviction worker thread evicting pages",
    "@serverStatus.wiredTiger.cache.hazard pointer check calls",
    "@serverStatus.wiredTiger.cursor.cursor search near calls",

    # overflow / lookaside pressure (on host demand) pre-4.4
    "@serverStatus.wiredTiger.cache.cache overflow score",
    "@serverStatus.wiredTiger.cache.cache overflow table entries",
    "@serverStatus.wiredTiger.cache.cache overflow table insert calls",
    "@serverStatus.wiredTiger.cache.cache overflow table remove calls",
    # "@serverStatus.wiredTiger.cache.cache overflow cursor internal thread wait time (usecs)"
    # new in 4.4:
    "@serverStatus.wiredTiger.cache.history store score",
    "@serverStatus.wiredTiger.cache.history store table on-disk size"
    "@serverStatus.wiredTiger.cache.history store table insert calls",
    "@serverStatus.wiredTiger.cache.history store table reads",

    "@local.oplog.rs.stats.wiredTiger.reconciliation.overflow values written",
    "@local.oplog.rs.stats.wiredTiger.btree.overflow pages",
    "@serverStatus.wiredTiger.cache.overflow pages read into cache",
    "@serverStatus.wiredTiger.cache.pages selected for eviction unable to be evicted as the parent page has overflow items",

    # WT backup cursors
    "serverStatus.storageEngine.backupCursorOpen",
    "serverStatus.storageEngine.oldestRequiredTimestampForCrashRecovery.t",
    "serverStatus.storageEngine.oldestRequiredTimestampForCrashRecovery.i",

    # dhandles
    "serverStatus.wiredTiger.data-handle.connection data handles currently active",
)


def pyftdc_pull_numpy_vectors(filepath, ftdc_metrics_keys, padZeros=False, t0=-1, t1=np.inf):
    import pyftdc

    p = pyftdc.FTDCParser()
    status = p.parse_file(filepath)
    assert status == 0

    fi = p.get_parsed_file_info()

    # for now, ignore padZeros
    metrics = p.metric_names
    keys_to_fetch = []
    for key in ftdc_metrics_keys:
        k = key[(key[0] == '@'):]
        if k in metrics:
            keys_to_fetch.append(key)

    # for now, ignore t0 and t1 just work with what's in ts array
    ts = p.get_timestamps()

    L = len(ts)
    #np_ftdc_data = p.get_metrics_list_numpy(
    #    keys_to_fetch,
    #    start = ts[int(L/2)-int(L/5)],
    #    end = ts[int(L/2)+int(L/5)])

    np_ftdc_data = p.get_metrics_list_numpy_matrix(
        keys_to_fetch,
        start=ts[int(L/2)-int(L/5)],
        end=ts[int(L/2)+int(L/5)],
        transpose=True
    )

    m = list()
    join_str = '.'
    for md in p.metadata:
        h = json.loads(md)
        if 'doc' in h.keys():
            #kk = h['doc'].keys()
            # for c in range(len(kk)):
            #    h['doc'][c] = join_str.join(list(k[c]))
            m.append(h['doc'])

    return {
        # return ftdc_data
        # shim as quasi- readers_numpy data structure
        'n_samples': len(np_ftdc_data[:, 0]),
        'ts_samples_start': np_ftdc_data[0, 0],
        'ts_samples_end': np_ftdc_data[-1, 0],
        # pass thru
        'rg_column_keys': np.array(keys_to_fetch),
        'rg_samples_numpy': np_ftdc_data,
        'ftdc_sysenv': m[-1],  # final frame expected for now
        'rg_ftdc_sysenv': m,  # all frames
    }


def test_stall():

    file = '/home/jorge/CLionProjects/mongo_ftdc/tests/diagnostic.data_40/metrics.2021-07-22T17-16-31Z-00000'
    v = pyftdc_pull_numpy_vectors(file, ftdc_metrics_keys)

    print("done")


