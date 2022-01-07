# -*- coding: utf-8 -*-

def test_import():
    import pyftdc
    assert 0 == 0

diagnostics_file = './diagnostic.data_40/metrics.2021-07-22T17-16-31Z-00000'

def test_parse():
    import pyftdc

    # Create a parser object
    p = pyftdc.FTDCParser()
    status = p.parse_file(diagnostics_file)

    assert status == 0


def test_parse_get_metadata():
    import pyftdc

    # Create a parser object
    p = pyftdc.FTDCParser()
    status = p.parse_file(diagnostics_file)

    assert status == 0

    meta = p.metadata
    if len(meta) > 0:
        print(meta[0])
    print(f"metadata has {len(meta)} elements")

    assert len(meta) > 0


def test_parse_get_timestamps():
    import pyftdc

    # Create a parser object
    p = pyftdc.FTDCParser()
    status = p.parse_file(diagnostics_file)

    assert status == 0

    ts = p.get_timestamps()

    print(f"There are {len(ts)} timestamps")
    assert len(ts) > 0


def test_parse_metrics():
    import pyftdc

    # Create a parser object
    p = pyftdc.FTDCParser()
    status = p.parse_file(diagnostics_file)

    assert status == 0
    metrics = p.metric_names

    for m in metrics:
        print(f"\tMetric: {m}")
    print(f"There are {len(metrics)} metrics")
    assert len(metrics) > 0


def test_metrics_samples():
    import pyftdc

    # Create a parser object
    p = pyftdc.FTDCParser()
    status = p.parse_file(diagnostics_file)
    assert status == 0

    metrics = p.metric_names
    m = p.get_metric(metrics[37])
    n = p.get_metric(metrics[73])

    assert len(n) == len(m)

    ts = p.get_timestamps()
    middle_ts = ts[int(len(ts)/2)]

    h1 = p.get_metric(metrics[73], end=middle_ts)
    h2 = p.get_metric(metrics[73], start=middle_ts)

    assert len(ts) == len(h1) + len(h2)

    # Ten samples (same chunk, for this metrics file)
    ten_more = ts[int(len(ts)/2)+10]
    m_10 = p.get_metric(metrics[37], start=middle_ts, end=ten_more)

    assert 10 == len(m_10)

    # Four hundred so me use two chunks (again, for this particular metrics file)
    four_hundred_more = ts[int(len(ts)/2)+400]
    m_400 = p.get_metric(metrics[37], start=middle_ts, end=four_hundred_more)
    assert 400 == len(m_400)


#TODO: Test for lists of samples

def test_metrics_numpy():
    import pyftdc

    # Create a parser object
    p = pyftdc.FTDCParser()
    status = p.parse_file(diagnostics_file)
    assert status == 0

    metrics = p.metric_names
    m = p.get_metric(metrics[37])
    n = p.get_metric(metrics[73])

    assert len(n) == len(m)

    ts = p.get_timestamps()
    middle_ts = ts[int(len(ts)/2)]

    h1 = p.get_metric_numpy(metrics[73], end=middle_ts)
    h2 = p.get_metric_numpy(metrics[73], start=middle_ts)

    assert len(ts) == len(h1) + len(h2)

    # Ten samples (same chunk, for this metrics file)
    ten_more = ts[int(len(ts)/2)+10]
    m_10 = p.get_metric_numpy(metrics[37], start=middle_ts, end=ten_more)

    assert 10 == len(m_10)

    # Four hundred so me use two chunks (again, for this particular metrics file)
    four_hundred_more = ts[int(len(ts)/2)+400]
    m_400 = p.get_metric_numpy(metrics[37], start=middle_ts, end=four_hundred_more)
    assert 400 == len(m_400)
    assert str(type(m_400)) == "<class 'numpy.ndarray'>"
    mm = p.get_metrics_list_numpy([metrics[73], metrics[37]])

    assert str(type(mm[0])) == "<class 'numpy.ndarray'>"
    assert len(mm) == 2
    assert len(mm[0]) == len(n)
    assert len(mm[1]) == len(m)


def test_metrics_rated_numpy():
    import pyftdc

    # Create a parser object
    p = pyftdc.FTDCParser()
    status = p.parse_file(diagnostics_file)
    assert status == 0

    metrics = p.metric_names
    m = p.get_metric(metrics[37])
    n = p.get_metric(metrics[73])

    assert len(n) == len(m)

    m_rated_with_name = p.get_metric('@'+metrics[37])
    m_rated = p.get_metric(metrics[37], rated_metric=True)

    assert len(m_rated_with_name) == len(m_rated)


def test_metrics_stall():
    import pyftdc

    # Create a parser object
    p = pyftdc.FTDCParser()
    status = p.parse_file(diagnostics_file)
    assert status == 0

    ftdc_metrics_keys = [
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
    ]

    mm = p.get_metrics_list_numpy(ftdc_metrics_keys)

    print(len(mm))

