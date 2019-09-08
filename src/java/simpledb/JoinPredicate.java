package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    private int leftIdx;
    private int rightIdx;
    private Predicate.Op pop;
    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param fieldIdx1
     *            The field index into the first tuple in the predicate
     * @param fieldIdx2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int fieldIdx1, Predicate.Op op, int fieldIdx2) {
        // some code goes here
        leftIdx = fieldIdx1;
        rightIdx = fieldIdx2;
        pop = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        return t1.getField(leftIdx).compare(this.pop,t2.getField(this.rightIdx));
    }
    
    public int getField1()
    {
        // some code goes here
        return leftIdx;
    }
    
    public int getField2()
    {
        // some code goes here
        return rightIdx;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return this.pop;
    }
}
