package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator opi;
    private int tableId;
    private TransactionId tid;
    private boolean insertFlag = false;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if (!checkInvariance(tableId, child))
            throw new DbException("TupleDesc is not equal");
        opi = child;
        tid = t;
        this.tableId = tableId;
    }

    private static boolean checkInvariance(final int tableId, final OpIterator child) {
        TupleDesc t1 = Database.getCatalog().getTupleDesc(tableId);
        TupleDesc t2 = child.getTupleDesc();
        return t1.equals(t2);
    }
    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE}); // indicated the number of inserted tuples
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        opi.open();
    }

    public void close() {
        // some code goes here
        super.close();
        opi.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
//        opi.rewind();
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (insertFlag)
            return null;
        try {
            int noInsert = 0;
            BufferPool bf = Database.getBufferPool();
            while (opi.hasNext()) {
                bf.insertTuple(this.tid, this.tableId, opi.next());
                noInsert++;
            }
            Tuple ret = new Tuple(this.getTupleDesc());
            ret.setField(0, new IntField(noInsert));
            insertFlag = true;//bad design
            return ret;
        } catch (IOException e) {
//            e.printStackTrace();
            throw new DbException("IO error");
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{opi};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        assert children.length == 1 : "the length of children should be 1";
        opi = children[0];
        if (checkInvariance(tableId, opi))
            throw new IllegalArgumentException();
    }
}
