package simpledb;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int agField;
//    private Op op;

    private HashMap<Field,Integer> gbAns;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what!=Op.COUNT)
            throw new IllegalArgumentException();
        this.agField = afield;
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
//        this.op = what;
        this.gbAns = new HashMap<>();
    }

    /**
     * NOTE: op must be COUNT
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field k = this.gbField == NO_GROUPING ? null : tup.getField(this.gbField);

        int val = gbAns.getOrDefault(k,0);
        gbAns.put(k,val+1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new TupleIterator(ansTupleDesc(),()->{return iterateTuple();});
    }
    public TupleDesc ansTupleDesc(){
        return this.gbField == NO_GROUPING? new TupleDesc(new Type[]{Type.INT_TYPE}) : new TupleDesc(new Type[]{this.gbFieldType,Type.INT_TYPE});
    }
    public Iterator<Tuple> iterateTuple(){
        if(this.gbField == NO_GROUPING)
            return this.gbAns.values().stream()
                .map(x -> {
                    TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
                    Tuple t = new Tuple(td);
                    t.setField(0,new IntField(x));
                    return t;
                }).iterator();
        else{
            return this.gbAns.entrySet().stream()
                .map(x ->{
                    TupleDesc td = new TupleDesc(new Type[]{this.gbFieldType,Type.INT_TYPE});
                    Tuple t = new Tuple(td);
                    t.setField(0,x.getKey());
                    t.setField(1,new IntField(x.getValue()));
                    return t;
                }).iterator();
        }
    }
 }
