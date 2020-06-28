package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked = false;
        private boolean endOfRight = false;
        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            SortOperator table1 = new SortOperator(SortMergeOperator.this.getTransaction(), this.getLeftTableName(),
                    new LeftRecordComparator());


            SortOperator table2 = new SortOperator(SortMergeOperator.this.getTransaction(), this.getRightTableName(),
                    new RightRecordComparator());

            this.leftIterator =  (BacktrackingIterator<Record>) table1.iterator();

            this.rightIterator = (BacktrackingIterator<Record>) table2.iterator();

            this.nextRecord = null;

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            // We mark the first record so we can reset to it when we advance the left record.
            if (rightRecord != null) {
                rightIterator.markPrev();
            } else { return; }

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }

            }



        private void fetchNextRecord() {
            this.nextRecord = null;
        if (this.leftIterator != null && this.rightIterator != null) {
            if (!this.marked) {
                while (0 > this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex()))) {
                    if (this.leftIterator.hasNext()) {
                        this.leftRecord = this.leftIterator.next();
                    } else {
                       this.nextRecord = null;
                        this.leftIterator = null;
                        return;
                    }
                }

                while (0 < this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex()))) {
                    if (this.rightIterator.hasNext()) {
                        this.rightRecord = this.rightIterator.next();
                    } else {
                        this.nextRecord = null;
                        this.rightIterator = null;
                        return;
                    }
                }
                this.rightIterator.markPrev();
                this.marked = true;

            }

            if ((!endOfRight) && 0 == this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                    this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex()))) {
                this.nextRecord = joinRecords(this.leftRecord, this.rightRecord);
                if (this.rightIterator.hasNext()) {
                    this.rightRecord = this.rightIterator.next();
                }
                else {
                    this.endOfRight = true;
                }
                    return;

            }
            else {
                this.rightIterator.reset();
                this.rightRecord = this.rightIterator.next();
                this.endOfRight = false;
                if (this.leftIterator.hasNext()) {
                    this.leftRecord = this.leftIterator.next();
                } else {
                    this.nextRecord = null;
                    this.leftIterator = null;
                    return;
                }
                this.marked = false;
                if (this.nextRecord == null) {
                this.fetchNextRecord(); }
            }
        }

        }


        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }
        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;

        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (this.nextRecord == null) {
                throw new NoSuchElementException();
            }
            Record recordToReturn = this.nextRecord;
            this.fetchNextRecord();
            return recordToReturn;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
