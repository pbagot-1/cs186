package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;
        // The next record to return
        private Record nextRecord = null;

        private boolean endOfLeft = false;

        private BNLJIterator() {
            super();

            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            this.rightIterator.markNext();
            fetchNextRightPage();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         *
         *
         public BacktrackingIterator<Record> getBlockIterator(String tableName, Iterator<Page> block,
         int maxPages) {
         return this.transaction.getBlockIterator(tableName, block, maxPages);
         }
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            if (!this.leftIterator.hasNext()) {
                this.leftRecordIterator = null;
                this.leftRecord = null;
                return;
            }

            do {
            this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), this.leftIterator,  BNLJOperator.this.numBuffers - 2); }
            while (!this.leftRecordIterator.hasNext() && this.leftIterator.hasNext());

            if (!this.leftRecordIterator.hasNext()) {
                this.leftRecordIterator = null;
                this.leftRecord = null;
                return;
            }
            else{
                this.leftRecordIterator.markNext();
                this.leftRecord = this.leftRecordIterator.next();
            }


            //this.leftRecordIterator.markPrev();
            //this.leftRecordIterator.reset();

        }

        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         *
         * If there are no more pages in the right relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            //if (!this.rightIterator.hasNext()) {
               // this.rightRecordIterator = null;
              //  return;
           // }
           // while (this.rightIterator.hasNext()) {
            do {
                this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), this.rightIterator, 1);
            } while(!this.rightRecordIterator.hasNext() && this.rightIterator.hasNext());
              //  if (this.rightRecordIterator.hasNext()) {

              //      break;
              //  }
          //  }
            if (/*rightRecordIterator == null ||*/ !this.rightRecordIterator.hasNext()) {
               // this.rightRecordIterator = null;
            } else {
                this.rightRecordIterator.markNext();
            }


        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */

        /**
         * reset() resets the iterator to the last marked location.
         *
         * The next next() call should return the value that was marked - if markPrev()
         * was used, this is the value returned by the next() call before markPrev(), and if
         * markNext() was used, this is the value returned by the next() call after markNext().
         * If neither mark methods were called, reset() does nothing. You may reset() to the same
         * point as many times as desired, as long as neither mark method is called again.
         */

        /**
         * markNext() marks the next returned value of the iterator, which is the
         * value returned by the next call of next().
         *
         * Calling markNext() on an iterator that has no records left,
         * or that has not yielded a record since the last reset() call does nothing.
         */

        /**
         * markPrev() marks the last returned value of the iterator, which is the last
         * returned value of next().
         *
         * Calling markPrev() on an iterator that has not yielded a record yet,
         * or that has not yielded a record since the last reset() call does nothing.
         */
        private void fetchNextRecord() {
            // TODO(proj3_part1): implement
            if (this.leftRecordIterator == null) {
                throw new NoSuchElementException();
            }



                        while (this.rightRecordIterator.hasNext()) {
                            Record current = this.rightRecordIterator.next();

                            //DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                            //DataBox rightJoinValue = current.getValues().get(BNLJOperator.this.getRightColumnIndex());

                            if (current.equals(this.leftRecord)) {
                                List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                                List<DataBox> rightValues = new ArrayList<>(current.getValues());
                                leftValues.addAll(rightValues);
                                this.nextRecord = new Record(leftValues);
                                return;
                            }
                           if (!this.rightRecordIterator.hasNext()) {
                               this.fetchNextRecord();
                           }
                        }
                            if (!this.rightRecordIterator.hasNext()) {
                                if (this.leftRecordIterator.hasNext()) {
                                    this.leftRecord = this.leftRecordIterator.next();
                                    this.rightRecordIterator.reset();
                                    this.rightRecordIterator.markNext();


                                    this.fetchNextRecord();

                                }
                                else {
                                    if (rightIterator.hasNext()) {
                                        this.fetchNextRightPage();
                                       // System.out.println("hix");
                                        this.leftRecordIterator.reset();
                                        this.leftRecordIterator.markNext();
                                        this.leftRecord = this.leftRecordIterator.next();
                                        this.fetchNextRecord();
                                        //this.endOfLeft = true;
                                    }
                                    else if (this.leftIterator.hasNext()){
                                        //System.out.println("hix");
                                        this.fetchNextLeftBlock();
                                        this.rightIterator.reset();
                                        this.rightIterator.markNext();
                                        fetchNextRightPage();
                                        this.fetchNextRecord();
                                    }
                                    throw new NoSuchElementException();
                                }
                            }




           /* //nothing was found in right for this record so move on, or done if no more
            if (this.leftRecordIterator.hasNext()) {
                this.leftRecord = this.leftRecordIterator.next();
                this.rightIterator.reset();

                //this.rightIterator.markNext();
                this.fetchNextRightPage();
                this.fetchNextRecord();
            }
            else {

                this.fetchNextLeftBlock();
                if (!(this.leftRecordIterator == null)) {
                    this.rightIterator.reset();
                    //this.rightIterator.markNext();
                    this.fetchNextRightPage();
                    this.fetchNextRecord();
                }
                else{
                    throw new NoSuchElementException();
                }


            }

            /*while (this.rightRecordIterator != null) {
                while (this.rightRecordIterator.hasNext()) {
                    Record current = this.rightRecordIterator.next();

                    //DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                    //DataBox rightJoinValue = current.getValues().get(BNLJOperator.this.getRightColumnIndex());

                    if (current.equals(this.leftRecord)) {
                        List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                        List<DataBox> rightValues = new ArrayList<>(current.getValues());
                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                        return;
                    }
                }
                this.fetchNextRightPage();
            }
            //nothing was found in right for this record so move on, or done if no more
            if (this.leftRecordIterator.hasNext()) {
                this.leftRecord = this.leftRecordIterator.next();
                this.rightIterator.reset();

                //this.rightIterator.markNext();
                this.fetchNextRightPage();
                this.fetchNextRecord();
            }
            else {

                this.fetchNextLeftBlock();
                if (!(this.leftRecordIterator == null)) {
                    this.rightIterator.reset();
                    //this.rightIterator.markNext();
                    this.fetchNextRightPage();
                    this.fetchNextRecord();
                }
                else{
                    throw new NoSuchElementException();
                }


            }*/


            //if (!(this.leftRecordIterator == null)) {

           // }
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
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
