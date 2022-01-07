import pyftdc
import numpy as np
import json
import os


def stack_vector(ftdc, v):
    return np.transpose(
        np.vstack((np.transpose(ftdc),
                   np.transpose(v)))).astype(np.int64)


def pyftdc_pull_numpy_vectors(filepath, ftdc_metrics_keys, padZeros=False, t0=-1, t1=np.inf):
    # test Jorge's file parser!
    # returns N-1 samples from N sampled
    # keys are ordered by ftdc_metrics_keys when available
    # optional pad 0 vector for missing ftdc keys
    # applies @-rated differentials
    # trims to timestamps range t0 <= t < t1

    # list the keys we want
    keys_to_fetch = []
    for key in ftdc_metrics_keys:
        k = key[(key[0] == '@'):]
        keys_to_fetch.append(k)

    p = pyftdc.FTDCParser()
    print("Reading up to %d FTDC metrics from %s ... " %
          (len(ftdc_metrics_keys), filepath))
    if os.path.isdir(filepath):
        s = p.parse_dir(filepath)
    else:
        s = p.parse_file(filepath)
    if s:
        print("error")
        return None
    print("done")
    m = list()
    for md in p.metadata:
        m.append(json.loads(md))
    # 2d array fetch, trim to requested interval range(s_ix)
    np_ftdc_data = np.transpose(p.get_metrics_list_numpy_matrix(keys_to_fetch, False))
    np_ftdc_data_x = p.get_metrics_list_numpy_matrix(keys_to_fetch, True)
    # ts = np.array(p.timestamps)
    ts0 = np_ftdc_data[:, 0]
    # trim rows by ts to fit requested ts interval range(s_ix)
    s_ix = np.intersect1d(np.argwhere(ts0 >= t0), np.argwhere(ts0 < t1))
    s_ix = s_ix[:-1]
    if(len(s_ix) == 0):
        return None
    # trim to ts range
    np_ftdc_data = np_ftdc_data[s_ix, ]
    ts = ts0[s_ix]
    print("Fetched %d samples of which %d are rated in the time range given having %d metadata frames" %
          (len(ts0), len(ts)-1, len(m)))
    # float elapsed unit seconds to rate deltas
    # dts = (ts0[1:] - ts0[:-1])/1000.0
    dts = (np_ftdc_data[1:, 0] - np_ftdc_data[:-1, 0])/1000.0
    dts = np.where(dts == 0, 1e-16, dts)
    # assume keys are column-ordered as requested
    ftdc_metrics_keys_returned = list()
    K = 0
    for key in ftdc_metrics_keys:
        k = key[(key[0] == '@'):]
        # as-available, ordered by ftdc_metrics_keys
        ftdc_metrics_keys_returned.append(key)
        if k in p.metric_names:
            #v = np.array(p.get_metric(k))
            v = np_ftdc_data[:, K]  # Kth column
            if key[0] == '@':  # apply rated
                vv = (v[1:] - v[0:-1]) / dts
            else:  # or shift 1 sample "up"
                vv = v[1:]
            np_ftdc_data[:-1, K] = vv  # replace column one row shorter
        elif padZeros:
            # insert column K of zeros
            np_ftdc_data = np.insert(np_ftdc_data, K, 0, axis=1)
        K += 1
    # trim off subtracted final row
    np_ftdc_data = np_ftdc_data[:-1, :]
    print("Done encoding 2D numpy frame")
    return {
        # return ftdc_data
        # shim as quasi- readers_numpy data structure
        'n_samples': len(np_ftdc_data[:, 0]),
        'ts_samples_start': np_ftdc_data[0, 0],
        'ts_samples_end': np_ftdc_data[-1, 0],
        'rg_column_keys': ftdc_metrics_keys_returned,  # pass thru
        'rg_samples_numpy': np_ftdc_data,
        'ftdc_sysenv': m,
    }

#
# unit tests
#


if __name__ == '__main__':

    ftdc_keys = (
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

    sampled_data = pyftdc_pull_numpy_vectors(
        '/home/jorge/CLionProjects/mongo_ftdc/tests/diagnostic.data_40/metrics.2021-07-22T17-16-31Z-00000', ftdc_keys)

    print("First metadata frame:")
    print(sampled_data['ftdc_sysenv'][0])
