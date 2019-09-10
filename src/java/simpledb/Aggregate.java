package simpledb;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private Aggregator.Op aop;
    private int afieldIdx;
    private int gbFieldIdx;

    // cache for child (mutable for child)
    private OpIterator collector;
    private TupleDesc childTd;
    private OpIterator child;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.childTd = child.getTupleDesc();
        this.afieldIdx = afield;
        this.gbFieldIdx = gfield;
        this.aop = aop;
        this.child = child;
        this.collector = this.collectFromChild();
    }

    private OpIterator collectFromChild() {
        Type aFieldType = childTd.getFieldType(this.afieldIdx);
        Type gbFieldType = this.gbFieldIdx == Aggregator.NO_GROUPING ? null : childTd.getFieldType(gbFieldIdx);
        Aggregator aggregator = aFieldType.equals(Type.INT_TYPE) ? new IntegerAggregator(gbFieldIdx, gbFieldType, afieldIdx, aop) :
            new StringAggregator(gbFieldIdx, gbFieldType, afieldIdx, aop);
        // do collection
        try {
            child.open();
            while (child.hasNext()) {
                aggregator.mergeTupleIntoGroup(child.next());
            }
            return aggregator.iterator();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } finally {
            child.close();
        }
        return null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // some code goes here
        return gbFieldIdx;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        // some code goes here
        return gbFieldIdx == Aggregator.NO_GROUPING ? null : childTd.getFieldName(gbFieldIdx);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // some code goes here
        return afieldIdx;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
        return childTd.getFieldName(afieldIdx);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        // some code goes here
        super.open();
        this.collector.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple ret = null;
        try {
            ret = collector.next();
        } catch (NoSuchElementException e) {
            ret = null;
        }
        return ret;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
//        super.rewind();
        this.close();
        this.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
        return gbFieldIdx != Aggregator.NO_GROUPING ? new TupleDesc(
            new Type[]{childTd.getFieldType(gbFieldIdx), childTd.getFieldType(afieldIdx)},
            new String[]{childTd.getFieldName(gbFieldIdx), String.format("%s (%s)", aop, childTd.getFieldName(afieldIdx))}) :
            new TupleDesc(
                new Type[]{childTd.getFieldType(afieldIdx)},
                new String[]{String.format("%s (%s)", aop, childTd.getFieldName(afieldIdx))}
            );
    }

    public void close() {
	// some code goes here
        super.close();
        collector.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        assert children.length == 1 : "the length of children must be 1";
        child = children[0];
        this.childTd = child.getTupleDesc();
        this.collector = collectFromChild();
    }
    
}
