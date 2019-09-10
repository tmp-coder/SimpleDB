package simpledb;

import java.io.Serializable;
import java.util.Objects;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private PageId pgId;
    /**
     * 0-based slot idx on page {@code pgId}
     */
    private int slotIdx;
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageId of the page on which the tuple resides
     * @param tupleNo
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleNo) {
        // some code goes here
        this.pgId = pid;
        this.slotIdx = tupleNo;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return slotIdx;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pgId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordId recordId = (RecordId) o;
        return slotIdx == recordId.slotIdx &&
            Objects.equals(pgId, recordId.pgId);
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return Integer.parseInt(String.valueOf(Objects.hash(pgId)) + String.valueOf(this.slotIdx));
    }
}
