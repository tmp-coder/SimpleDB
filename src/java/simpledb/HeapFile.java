package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile. same as tableId
     */
    public int getId() {
        // some code goes here
        int primer = 31;
        return primer * file.getAbsoluteFile().hashCode() + td.hashCode() ;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs

    /**
     * Read the specified page from disk.
     * @param pid specified pageId
     * @return Page a heapPage
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    public Page readPage(PageId pid) {
        // some code goes here
        int pgNo = pid.getPageNumber();
        if(pgNo>=numPages())
            throw new IllegalArgumentException();

        int pgSize = BufferPool.getPageSize();
        byte[] data = new byte[pgSize];
        HeapPage ret = null;
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long offset = pgSize *(long) pgNo;
            raf.seek(offset);
            raf.read(data);
            ret = new HeapPage(new HeapPageId(pid.getTableId(),pgNo),data);
        }catch (IOException e){
            e.printStackTrace();
            System.exit(1);
        }
        return ret;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = BufferPool.getPageSize();

        return (int)(file.length() /pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private int currentPgNo = 0;
        private final TransactionId tid;
        private final int numPages;
        private Iterator<Tuple> tupleIterator;
        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            numPages = numPages();
            tupleIterator = null;
        }

        private Iterator<Tuple> getNextPageTuples() throws TransactionAbortedException, DbException {
            if(currentPgNo >= numPages)
                throw new NoSuchElementException("no more pages");
            PageId pid = new HeapPageId(getId(),currentPgNo);
            HeapPage page =  (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY); // bad design
                                                                    // ,maybe page should has Iterator method is better
            currentPgNo++;
            return page.iterator();
        }

        /**
         * Opens the iterator
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            currentPgNo = 0;
            tupleIterator = getNextPageTuples();
        }

        /**
         * @return true if there are more tuples available, false if no more tuples or iterator isn't open.
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tupleIterator == null)
                return false; // bad design, just for pass stupid unit test
//            if(tupleIterator.hasNext())
//                return true;
            while (!tupleIterator.hasNext() && currentPgNo < numPages){
                // access next page
                tupleIterator = getNextPageTuples();
            }
            return tupleIterator.hasNext();
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(tupleIterator ==null)
                throw new NoSuchElementException();// just for bad test
            while (!tupleIterator.hasNext()){
                tupleIterator = getNextPageTuples();
            }
            return tupleIterator.next();
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.open();
        }

        /**
         * Closes the iterator.
         */
        @Override
        public void close() {
            tupleIterator = null;
        }
    }
}

