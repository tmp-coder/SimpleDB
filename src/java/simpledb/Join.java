package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator[] children;
    private JoinPredicate jp;

    // caches
    private Tuple[] tuplesOfChild2;
    private int curIdxOfChild2;
    private Tuple leftTuple;
    private TupleDesc retTd;
    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        jp = p;
        children = new OpIterator[]{child1,child2};

        initCaches(children);

    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return jp;
    }

    private String getJoinFieldName(int childIdx){
        return children[0].getTupleDesc().getFieldName(childIdx>0?jp.getField2():jp.getField1());
    }
    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return getJoinFieldName(0);
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return getJoinFieldName(1);
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return retTd;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        children[0].open();
        curIdxOfChild2 =0;
    }

    public void close() {
        super.close();
        children[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
//        super.rewind();
        super.open();
        children[0].rewind();
        curIdxOfChild2 =0;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */

    private Tuple mergeJoinTuple(Tuple t1,Tuple t2){
        Tuple ret = new Tuple(this.retTd);
        int t1Size = t1.getTupleDesc().numFields();
        for(int i=0 ; i< t1Size ; ++i)
            ret.setField(i,t1.getField(i));
        for(int i=0 ; i<t2.getTupleDesc().numFields() ; ++i)
            ret.setField(i+t1Size,t2.getField(i));

        return ret;
    }

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
//        return null;

        try{
            if (leftTuple == null)
                leftTuple = children[0].next();
            while (true){
                try{
                    Tuple rightTuple = tuplesOfChild2[curIdxOfChild2++];
                    while (!jp.filter(leftTuple,rightTuple))
                        rightTuple = tuplesOfChild2[curIdxOfChild2++];
                    return mergeJoinTuple(leftTuple,rightTuple);
                }catch (ArrayIndexOutOfBoundsException e){// rewind 2
                    curIdxOfChild2 =0;
                    leftTuple = children[0].next();
                }
            }
        }catch (NoSuchElementException e){
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
        // init caches
        initCaches(children);
    }

    private void initCaches(OpIterator[] children){
        retTd = TupleDesc.merge(children[0].getTupleDesc(),children[1].getTupleDesc());
        try {
            children[1].open();
            ArrayList<Tuple> tmp = new ArrayList<>();// very bad if database is very big
            curIdxOfChild2 =0;
            while (children[1].hasNext())
                tmp.add(children[1].next());
            children[1].close();
            tuplesOfChild2 = tmp.toArray(new Tuple[0]);
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
    }
}
