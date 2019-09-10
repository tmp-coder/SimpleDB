package simpledb;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;


    private int gbField;
    private Type gbFieldType;
    private int agField;
    private Op gbOp;

    private HashMap<Field, Aggregator.Stat> gbAns;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.agField = afield;
        this.gbOp = what;
        gbAns = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Integer agVal = ((IntField)tup.getField(this.agField)).getValue();

        Field k = null;
        if (this.gbField != NO_GROUPING)
            k = tup.getField(this.gbField);
        if (!gbAns.containsKey(k))
            gbAns.put(k, new Stat());
        gbAns.get(k).insert(this.gbOp, agVal);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new TupleIterator(constructTd(),iterateAns());
    }
    private TupleDesc constructTd() {
        return this.gbField == NO_GROUPING? new TupleDesc(new Type[]{Type.INT_TYPE}) : new TupleDesc(new Type[]{this.gbFieldType,Type.INT_TYPE});
    }
    private Iterator<Tuple> toIterator(){
        TupleDesc td;
        if(this.gbField == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            return gbAns.values().stream()
                .map(x -> {
                    Tuple t = new Tuple(td);
                    t.setField(0, new IntField(x.rtAns(this.gbOp)));
                    return t;
                }).iterator();
        }else {
            td = new TupleDesc(new Type[]{this.gbFieldType,Type.INT_TYPE});
            return gbAns.entrySet()
                .stream()
                .map(x ->{
                    Tuple t = new Tuple(td);
                    t.setField(0,x.getKey());
                    t.setField(1,new IntField(x.getValue().rtAns(this.gbOp)));
                    return t;
                }).iterator();
        }
    }
    private Iterable<Tuple> iterateAns(){

            return new Iterable<Tuple>() {
                /**
                 * Returns an iterator over elements of type {@code T}.
                 *
                 * @return an Iterator.
                 */
                @Override
                public Iterator<Tuple> iterator() {
                    return toIterator();
                }
            };
    }
}


