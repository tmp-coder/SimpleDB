package simpledb;

import java.io.Serializable;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    int NO_GROUPING = -1;

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    void mergeTupleIntoGroup(Tuple tup);

    /**
     * SUM_COUNT and SC_AVG will
     * only be used in lab7, you are not required
     * to implement them until then.
     */
    public enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT,
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be
         * needed to compute distributed avg in lab7.
         */
        SUM_COUNT,
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
         * will be used to compute distributed avg in lab7.
         */
        SC_AVG;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == MIN)
                return "min";
            if (this == MAX)
                return "max";
            if (this == SUM)
                return "sum";
            if (this == SUM_COUNT)
                return "sum_count";
            if (this == AVG)
                return "avg";
            if (this == COUNT)
                return "count";
            if (this == SC_AVG)
                return "sc_avg";
            throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @see simpledb.TupleIterator for a possible helper
     */
    OpIterator iterator();

    public static class Stat{
        private int cnt;
        private int val;

        public Stat() {
            cnt = -1;// indicate not initialize
        }
        public Stat(int val){
            this.val = val;
            cnt=1;
        }
        private void init(Op op){
            cnt = 0;
            switch (op){
                case MIN:
                    val = Integer.MAX_VALUE;
                    break;
                case MAX:
                    val = Integer.MIN_VALUE;
                    break;
                default:
                    val =0;
            }
        }
        public void insert(Op op,int val){
            if(cnt ==-1)
                init(op);
            switch (op){
                case MAX:
                    if(this.val < val)
                        this.val = val;
                    break;
                case MIN:
                    if(this.val > val)
                        this.val = val;
                    break;
                default:
                    this.val += val;
                    cnt++;
            }
        }

        public int rtAns(Op op){
            switch (op){
                case AVG:
                    return this.val/this.cnt;
                case COUNT:
                    return cnt;
                default:
                    return this.val;
            }
        }
    }
    
}
