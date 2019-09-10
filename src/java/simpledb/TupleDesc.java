package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;
        private static final String DEFAULT_FIELD_NAME = "null";
        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public boolean equals(Object o){
            if(this == o)
                return true;
            if(o == null ||o.getClass() != this.getClass())
                return false;
            TDItem tdi = (TDItem)o;
//            return this.fieldType.equals(tdi.fieldType) && Objects.equals(tdi.fieldName,this.fieldName);
            return this.fieldType.equals(tdi.fieldType);
        }

        public int hashCode(){
            return (fieldName==null?0 : fieldName.hashCode()) * 31 + fieldType.hashCode();
        }
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return schema.iterator();
    }

    private static final long serialVersionUID = 1L;

    private ArrayList<TDItem> schema;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        assert typeAr.length >=1 :"typeArray must contain at least one entry";
        assert typeAr.length == fieldAr.length : "the length of type array and field array is not equal";
        schema = new ArrayList<>(typeAr.length);
        for(int i=0 ; i< typeAr.length ; ++i)
            schema.add(new TDItem(typeAr[i],fieldAr[i]));
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        assert typeAr.length >=1 :"typeArray must contain at least one entry";
        schema = new ArrayList<>(typeAr.length);
        for(int i=0 ; i< typeAr.length; ++i)
            schema.add(new TDItem(typeAr[i],null));
    }

    /**
     * private constructor, just for merge easy
     */
    private TupleDesc(){}
    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return schema.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        try{
            return schema.get(i).fieldName;
        }catch (ArrayIndexOutOfBoundsException e){
            throw new NoSuchElementException(e.getMessage());
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        try{
            return schema.get(i).fieldType;
        }catch (ArrayIndexOutOfBoundsException e){
            throw new NoSuchElementException(e.getMessage());
        }
    }

    /**
     * Find the index of the field with a given name.
     * NOTE: this method is much slow, may consume O(n) time
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {

        for(int i=0 ; i< schema.size() ; ++i)
            if(Objects.equals(name,schema.get(i).fieldName))
                return i;
        throw new NoSuchElementException(name + "is not valid fields name");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return schema.parallelStream()
            .map(x -> x.fieldType.getLen())
            .mapToInt(x->x)
            .sum();
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        TupleDesc td = new TupleDesc();
        td.schema = new ArrayList<>();
        td.schema.addAll(td1.schema);
        td.schema.addAll(td2.schema);
        return td;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if(o==null)return false;
        if(o == this)
            return true;
        if(o.getClass() != this.getClass())
            return false;

        TupleDesc td = (TupleDesc)o;
        return td.schema.equals(this.schema);
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results

        return schema.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {

        return schema.stream()
            .map(x -> String.format("%s(%s)",x.fieldType,x.fieldName))
            .collect(Collectors.joining(","));
    }
}
