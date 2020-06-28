package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);

        Long newLSN = this.logManager.appendToLog(new CommitTransactionLogRecord(transNum, transactionEntry.lastLSN));
        transactionEntry.lastLSN = newLSN;
        transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        this.logManager.flushToLSN(newLSN);

        return newLSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);

        Long newLSN = this.logManager.appendToLog(new AbortTransactionLogRecord(transNum, transactionEntry.lastLSN));
        transactionEntry.lastLSN = newLSN;
        transactionEntry.transaction.setStatus(Transaction.Status.ABORTING);


        return newLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        if (transactionEntry.transaction.getStatus() == Transaction.Status.ABORTING) {
            Optional<Long> LSNsweep;
            LogRecord check = this.logManager.fetchLogRecord(transactionEntry.lastLSN);
            do {//pecial dirty page table updates for UndoUpdatePageRecord and for UndoAllocPageRecord
                //LogRecord check = this.logManager.fetchLogRecord(LSNsweep);
                if (check.isUndoable()) {
                    Pair<LogRecord, Boolean> ret = check.undo(transactionEntry.lastLSN);

                    this.logManager.appendToLog(ret.getFirst());

                    if (ret.getSecond() == true) {
                        this.logManager.flushToLSN(ret.getFirst().getLSN());
                    }
                    ret.getFirst().redo(this.diskSpaceManager, this.bufferManager);
                    if (ret.getFirst().getType() == LogType.UNDO_UPDATE_PAGE) {
                        dirtyPageTable.putIfAbsent(ret.getFirst().getPageNum().get(), ret.getFirst().getLSN());
                    }
                    if (ret.getFirst().getType() == LogType.UNDO_ALLOC_PAGE) {
                        dirtyPageTable.remove(ret.getFirst().getPageNum().get());
                    }

                    transactionEntry.lastLSN = ret.getFirst().getLSN();


                }
                LSNsweep = check.getPrevLSN();
                if (LSNsweep.isPresent()) {
                    check = this.logManager.fetchLogRecord(LSNsweep.get());
                }

            }while (LSNsweep.isPresent() && LSNsweep.get() != 0);

            LogRecord endRecord = new EndTransactionLogRecord(transNum, transactionEntry.lastLSN);
            this.logManager.appendToLog(endRecord);
            transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
            transactionTable.remove(transNum);
            transactionEntry.lastLSN = endRecord.getLSN();
            return endRecord.getLSN();

        } else {
            LogRecord endRecord = new EndTransactionLogRecord(transNum, transactionEntry.lastLSN);
            this.logManager.appendToLog(endRecord);
            transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
            transactionTable.remove(transNum);
            transactionEntry.lastLSN = endRecord.getLSN();
            return endRecord.getLSN();
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);

        if (after.length > BufferManager.EFFECTIVE_PAGE_SIZE / 2) {
            byte[] empty = new byte[]{};
            LogRecord undoOnly = new UpdatePageLogRecord(transNum, pageNum, transactionEntry.lastLSN, pageOffset, before, empty);
            LogRecord redoOnly = new UpdatePageLogRecord(transNum, pageNum, transactionEntry.lastLSN, pageOffset, empty, after);
            this.logManager.appendToLog(undoOnly);
            this.logManager.appendToLog(redoOnly);
            dirtyPageTable.putIfAbsent(pageNum, undoOnly.getLSN());
            transactionEntry.lastLSN = redoOnly.getLSN();
            transactionEntry.touchedPages.add(pageNum);
            return redoOnly.getLSN();
        } else {
            LogRecord updateRec = new UpdatePageLogRecord(transNum, pageNum, transactionEntry.lastLSN, pageOffset, before, after);
            this.logManager.appendToLog(updateRec);
            dirtyPageTable.putIfAbsent(pageNum, updateRec.getLSN());
            transactionEntry.lastLSN = updateRec.getLSN();
            transactionEntry.touchedPages.add(pageNum);
            return updateRec.getLSN();
        }
        //transactionEntry.transaction.setStatus(Transaction.Status.RUNNING);
        // TODO(proj5): implement
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        LogRecord last = this.logManager.fetchLogRecord(transactionEntry.lastLSN);
        do {
            if (last.getLSN() > LSN) {
                if (last.isUndoable()) {
                    Pair<LogRecord, Boolean> ret = last.undo(transactionEntry.lastLSN);
                    this.logManager.appendToLog(ret.getFirst());

                    if (ret.getSecond() == true) {
                        this.logManager.flushToLSN(ret.getFirst().getLSN());
                    }
                    ret.getFirst().redo(this.diskSpaceManager, this.bufferManager);
                    if (ret.getFirst().getType() == LogType.UNDO_UPDATE_PAGE) {
                        dirtyPageTable.putIfAbsent(ret.getFirst().getPageNum().get(), ret.getFirst().getLSN());
                    }
                    if (ret.getFirst().getType() == LogType.UNDO_ALLOC_PAGE) {
                        dirtyPageTable.remove(ret.getFirst().getPageNum().get());
                    }

                    transactionEntry.lastLSN = ret.getFirst().getLSN();
                    if (last.getPrevLSN().get() > LSN) {
                        last = this.logManager.fetchLogRecord(last.getPrevLSN().get());
                    } else{
                        break;
                    }
                }
            } else {
                break;
            }
        } while (true);

    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        for (Map.Entry<Long, Long> entry: dirtyPageTable.entrySet()) {
            // Dirty page table (page number -> recLSN).
            //Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
            //long transNum = entry.getKey();

            boolean fitsAfterAdd;

            fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(dpt.size() + 1, txnTable.size(), touchedPages.size(), 0);
            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();

            }
            dpt.put(entry.getKey(), entry.getValue());

        }
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {


            boolean fitsAfterAdd;

            fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(dpt.size(), txnTable.size() + 1, touchedPages.size(), 0);

            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();

            }
            txnTable.put(entry.getKey(), new Pair<>(entry.getValue().transaction.getStatus(), entry.getValue().lastLSN));

        }
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    // TODO(proj5): add any helper methods needed

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        restartAnalysis();
        restartRedo();
        Set<Long> dirtyPageNums = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) {
                dirtyPageNums.add(pageNum);
            }
        });
        for (Long key : dirtyPageTable.keySet()) {
            if (!dirtyPageNums.contains(key)) {
                dirtyPageTable.remove(key);
            }
        }

        // TODO(proj5): implement
        return () -> {restartUndo();
            checkpoint();};
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        // TODO(proj5): implement
        Iterator<LogRecord> iter = logManager.scanFrom(LSN);
        while(iter.hasNext()) {
            LogRecord cur = iter.next();
            if (cur.type == LogType.ALLOC_PAGE || cur.type == LogType.FREE_PAGE || cur.type == LogType.UNDO_ALLOC_PAGE || cur.type == LogType.UNDO_FREE_PAGE) {
                transactionTable.putIfAbsent(cur.getTransNum().get(), new TransactionTableEntry(newTransaction.apply(cur.getTransNum().get())));
                transactionTable.get(cur.getTransNum().get()).lastLSN = cur.getLSN();
                transactionTable.get(cur.getTransNum().get()).touchedPages.add(cur.getPageNum().get());

                LockContext thisContext = getPageLockContext(cur.getPageNum().get());
                acquireTransactionLock(transactionTable.get(cur.getTransNum().get()).transaction, thisContext, LockType.X);
                dirtyPageTable.remove(cur.getPageNum().get());
            }
            else if (cur.type == LogType.UPDATE_PAGE || cur.type == LogType.UNDO_UPDATE_PAGE) {
                transactionTable.putIfAbsent(cur.getTransNum().get(), new TransactionTableEntry(newTransaction.apply(cur.getTransNum().get())));
                transactionTable.get(cur.getTransNum().get()).lastLSN = cur.getLSN();
                transactionTable.get(cur.getTransNum().get()).touchedPages.add(cur.getPageNum().get());

                LockContext thisContext = getPageLockContext(cur.getPageNum().get());
                acquireTransactionLock(transactionTable.get(cur.getTransNum().get()).transaction, thisContext, LockType.X);
                dirtyPageTable.putIfAbsent(cur.getPageNum().get(), cur.getLSN());

            }
          else if (cur.type == LogType.ALLOC_PART || cur.type == LogType.UNDO_ALLOC_PART || cur.type == LogType.FREE_PART || cur.type == LogType.UNDO_FREE_PART) {
                transactionTable.putIfAbsent(cur.getTransNum().get(), new TransactionTableEntry(newTransaction.apply(cur.getTransNum().get())));
                transactionTable.get(cur.getTransNum().get()).lastLSN = cur.getLSN();
            }else if (cur.type == LogType.COMMIT_TRANSACTION) {
                transactionTable.putIfAbsent(cur.getTransNum().get(), new TransactionTableEntry(newTransaction.apply(cur.getTransNum().get())));
                transactionTable.get(cur.getTransNum().get()).lastLSN = cur.getLSN();
                transactionTable.get(cur.getTransNum().get()).transaction.setStatus(Transaction.Status.COMMITTING);
            }
            else if (cur.type == LogType.ABORT_TRANSACTION) {
                transactionTable.putIfAbsent(cur.getTransNum().get(), new TransactionTableEntry(newTransaction.apply(cur.getTransNum().get())));
                transactionTable.get(cur.getTransNum().get()).lastLSN = cur.getLSN();
                transactionTable.get(cur.getTransNum().get()).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }
            else if (cur.type == LogType.END_TRANSACTION) {
                transactionTable.putIfAbsent(cur.getTransNum().get(), new TransactionTableEntry(newTransaction.apply(cur.getTransNum().get())));
                transactionTable.get(cur.getTransNum().get()).lastLSN = cur.getLSN();
                transactionTable.get(cur.getTransNum().get()).transaction.cleanup();
                transactionTable.get(cur.getTransNum().get()).transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(cur.getTransNum().get());
            }
            else if (cur.type == LogType.BEGIN_CHECKPOINT) {
                updateTransactionCounter.accept(Math.max(getTransactionCounter.get(), cur.getMaxTransactionNum().get()));

            }
            else if (cur.type == LogType.END_CHECKPOINT) {


                for (Map.Entry<Long, Long> entry : cur.getDirtyPageTable().entrySet()) {
                    this.dirtyPageTable.put(entry.getKey(), entry.getValue());
                }
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : cur.getTransactionTable().entrySet()) {
                    if (!this.transactionTable.keySet().contains(entry.getKey())) {
                        Transaction newT = newTransaction.apply(entry.getKey());
                        startTransaction(newT);
                        this.transactionTable.get(entry.getKey()).lastLSN = entry.getValue().getSecond();
                    } else {
                        if (this.transactionTable.get(entry.getKey()).lastLSN <=  entry.getValue().getSecond()) {
                            this.transactionTable.get(entry.getKey()).lastLSN =  entry.getValue().getSecond();
                        }
                    }
                }

                        //for (Long pageNum : cur.getTransactionTouchedPages().get(cur.getTransNum().get())) {
                for (Long transNum : cur.getTransactionTable().keySet()) {
                    if (transactionTable.get(transNum).transaction.getStatus() != Transaction.Status.COMPLETE) {
                        for (Long touched: (cur.getTransactionTouchedPages().get(transNum))) {
                            this.transactionTable.get(transNum).touchedPages.add(touched);
                            LockContext thisContext = getPageLockContext(touched);
                            acquireTransactionLock(transactionTable.get(transNum).transaction, thisContext, LockType.X);
                        }

                    }
                }
            }
    }

        for (Long key: transactionTable.keySet()) {
            if (transactionTable.get(key).transaction.getStatus() == Transaction.Status.COMMITTING) {
                transactionTable.get(key).transaction.cleanup();
                transactionTable.get(key).transaction.setStatus(Transaction.Status.COMPLETE);
                Long lsn = this.logManager.appendToLog(new EndTransactionLogRecord(key, transactionTable.get(key).lastLSN));
                transactionTable.get(key).lastLSN = lsn;
                transactionTable.remove(key);
            }
            if (transactionTable.get(key).transaction.getStatus() == Transaction.Status.RUNNING) {
                transactionTable.get(key).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                Long lsn = this.logManager.appendToLog(new AbortTransactionLogRecord(key, transactionTable.get(key).lastLSN));
                transactionTable.get(key).lastLSN = lsn;
            }
        }


        return;
        /*     // master log record (stores current checkpoint)
    MASTER,
    // log record for allocating a new page (via the disk space manager)
    ALLOC_PAGE,
    // log record for updating part of a page
    UPDATE_PAGE,
    // log record for freeing a page (via the disk space manager)
    FREE_PAGE,
    // log record for allocating a new partition (via the disk space manager)
    ALLOC_PART,
    // log record for freeing a partition (via the disk space manager)
    FREE_PART,
    // log record for starting a transaction commit
    COMMIT_TRANSACTION,
    // log record for starting a transaction abort
    ABORT_TRANSACTION,
    // log record for after a transaction has completely finished
    END_TRANSACTION,
    // log record for start of a checkpoint
    BEGIN_CHECKPOINT,
    // log record for finishing a checkpoint; there may be multiple of these
    // for a checkpoint
    END_CHECKPOINT,
    // compensation log record for undoing a page alloc
    UNDO_ALLOC_PAGE,
    // compensation log record for undoing a page update
    UNDO_UPDATE_PAGE,
    // compensation log record for undoing a page free
    UNDO_FREE_PAGE,
    // compensation log record for undoing a partition alloc
    UNDO_ALLOC_PART,
    // compensation log record for undoing a partition free
    UNDO_FREE_PART; */
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {

        Long start = Long.MAX_VALUE;
        for (Long key : dirtyPageTable.keySet()) {
            if (dirtyPageTable.get(key) < start) {
                start = dirtyPageTable.get(key);
            }
        }
        Iterator<LogRecord> iter = logManager.scanFrom(start);
        while(iter.hasNext()) {
            LogRecord cur = iter.next();
            if (cur.isRedoable()) {
                if (cur.type == LogType.ALLOC_PART || cur.type == LogType.UNDO_ALLOC_PART || cur.type == LogType.FREE_PART || cur.type == LogType.UNDO_FREE_PART) {
                    cur.redo(diskSpaceManager, bufferManager);
                }
                if (cur.type == LogType.ALLOC_PAGE || cur.type == LogType.UPDATE_PAGE || cur.type == LogType.FREE_PAGE || cur.type == LogType.UNDO_ALLOC_PAGE || cur.type == LogType.UNDO_UPDATE_PAGE || cur.type == LogType.UNDO_FREE_PAGE ) {
                    if (dirtyPageTable.containsKey(cur.getPageNum().get()) && cur.getLSN() >= dirtyPageTable.get(cur.getPageNum().get()) && bufferManager.fetchPage(getPageLockContext(cur.getPageNum().get()).parentContext(),cur.getPageNum().get(),false).getPageLSN() < cur.getLSN()) {
                        cur.redo(diskSpaceManager, bufferManager);
                    }
                }
            }
        }

        // TODO(proj5): implement
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        PriorityQueue<Long> myPQ = new PriorityQueue<>();
        for (Long key: transactionTable.keySet()) {
            if (transactionTable.get(key).transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                myPQ.add(transactionTable.get(key).lastLSN);
            }
        }
        while (!myPQ.isEmpty()) {
            LogRecord rec = logManager.fetchLogRecord(myPQ.poll());
            if (rec.isUndoable()) {
                Pair<LogRecord, Boolean> comp = rec.undo(transactionTable.get(rec.getTransNum().get()).lastLSN);
                this.logManager.appendToLog(comp.getFirst());
                if (comp.getSecond() == true) {
                    this.logManager.flushToLSN(comp.getFirst().getLSN());
                }
                comp.getFirst().redo(this.diskSpaceManager, this.bufferManager);
            }
            if (rec.getUndoNextLSN().isPresent()) {
                if (rec.getUndoNextLSN().get() == 0) {
                    transactionTable.remove(rec.getTransNum().get());
                } else {
                myPQ.add(rec.getUndoNextLSN().get()); }
            } else {
                if (rec.getPrevLSN().get() == 0) {
                    transactionTable.remove(rec.getTransNum().get());
                } else {
                myPQ.add(rec.getPrevLSN().get()); }
            }
        }
        return;
    }

    // TODO(proj5): add any helper methods needed

    // Helpers ///////////////////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
