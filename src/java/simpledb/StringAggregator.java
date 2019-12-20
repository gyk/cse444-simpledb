package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield; // unused
    private final Op what; // unused

    private TreeMap<Field, Integer> countMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.countMap = new TreeMap<>(Comparator.nullsFirst(Comparator.naturalOrder()));
    }

    public boolean hasGrouping() {
        return this.gbfield != Aggregator.NO_GROUPING;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupVal;
        if (hasGrouping()) {
            groupVal = tup.getField(this.gbfield);
        } else {
            groupVal = null;
        }
        int count = this.countMap.getOrDefault(groupVal, 0);
        this.countMap.put(groupVal, count + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal) if using group, or a single
     * (aggregateVal) if no grouping. The aggregateVal is determined by the type of aggregate specified in the
     * constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new DbIterator() {
            TupleDesc tupleDesc;
            Iterator<Map.Entry<Field, Integer>> iter;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                StringAggregator parent = StringAggregator.this;
                if (!parent.hasGrouping()) {
                    this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
                } else {
                    this.tupleDesc = new TupleDesc(
                            new Type[]{parent.gbfieldtype, Type.INT_TYPE});
                }

                this.iter = parent.countMap.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return this.iter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Map.Entry<Field, Integer> entry = this.iter.next();
                Field groupVal = entry.getKey();
                int aggregateVal = entry.getValue();

                StringAggregator parent = StringAggregator.this;
                Tuple res = new Tuple(this.tupleDesc);
                if (parent.hasGrouping()) {
                    res.setField(0, groupVal);
                    res.setField(1, new IntField(aggregateVal));
                } else {
                    res.setField(0, new IntField(aggregateVal));
                }

                return res;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return this.tupleDesc;
            }

            @Override
            public void close() {
            }
        };
    }

}
