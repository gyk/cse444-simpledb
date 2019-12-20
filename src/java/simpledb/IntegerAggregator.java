package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    public class AggregateState {
        public int min;
        public int max;
        public int sum;
        public int count;

        public AggregateState() {
            this.min = Integer.MAX_VALUE;
            this.max = Integer.MIN_VALUE;
            this.sum = 0;
            this.count = 0;
        }
    }

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private TreeMap<Field, AggregateState> stateMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.stateMap = new TreeMap<>(Comparator.nullsFirst(Comparator.naturalOrder()));
    }

    public boolean hasGrouping() {
        return this.gbfield != Aggregator.NO_GROUPING;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field g;
        if (hasGrouping()) {
            g = tup.getField(this.gbfield);
        } else {
            g = null;
        }

        AggregateState s = this.stateMap.getOrDefault(g, new AggregateState());

        IntField a = (IntField) tup.getField(this.afield);
        int v = a.getValue();

        s.max = Integer.max(s.max, v);
        s.min = Integer.min(s.min, v);
        s.sum += v;
        s.count++;

        this.stateMap.put(g, s);
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
            Iterator<Map.Entry<Field, AggregateState>> iter;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                IntegerAggregator parent = IntegerAggregator.this;
                if (!parent.hasGrouping()) {
                    this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
                } else {
                    this.tupleDesc = new TupleDesc(
                            new Type[]{parent.gbfieldtype, Type.INT_TYPE});
                }

                this.iter = parent.stateMap.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return this.iter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                IntegerAggregator parent = IntegerAggregator.this;
                Map.Entry<Field, AggregateState> entry = this.iter.next();
                Field groupVal = entry.getKey();
                AggregateState aggState = entry.getValue();
                int aggregateVal;
                switch (parent.what) {
                    case MIN:
                        aggregateVal = aggState.min;
                        break;
                    case MAX:
                        aggregateVal = aggState.max;
                        break;
                    case SUM:
                        aggregateVal = aggState.sum;
                        break;
                    case AVG:
                        aggregateVal = aggState.sum / aggState.count;
                        break;
                    case COUNT:
                        aggregateVal = aggState.count;
                        break;
                    case SUM_COUNT:
                        throw new UnsupportedOperationException();
                    case SC_AVG:
                        throw new UnsupportedOperationException();
                    default:
                        throw new UnsupportedOperationException();
                }

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
