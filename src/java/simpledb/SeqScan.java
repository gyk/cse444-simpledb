package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private int tableid;
    private String tableAlias;
    private TransactionId tid;
    private DbFileIterator dbFileIterator;
    private TupleDesc tupleDesc;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned tupleDesc should have fields with
     *                   name tableAlias.fieldName (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they are, but the resulting name can be
     *                   null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        reset(tableid, tableAlias);
    }

    /**
     * @return return the table name of the table the operator scans. This should be the actual name of the table in the
     * catalog of the database
     */
    public String getTableName() {
        // some code goes here
        return Database.getCatalog().getTableName(this.tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned tupleDesc should have fields with
     *                   name tableAlias.fieldName (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they are, but the resulting name can be
     *                   null.fieldName, tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;

        TupleDesc origTd = Database.getCatalog().getTupleDesc(tableid);
        ArrayList<TupleDesc.TDItem> tdList = new ArrayList<>(origTd.numFields());
        origTd.iterator().forEachRemaining(tdItem -> {
            tdList.add(new TupleDesc.TDItem(tdItem.fieldType, tableAlias + tdItem.fieldName));
        });
        TupleDesc.TDItem[] tdItems = new TupleDesc.TDItem[tdList.size()];
        tdList.toArray(tdItems);
        this.tupleDesc = new TupleDesc(tdItems);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDatabaseFile(this.tableid);
        this.dbFileIterator = dbFile.iterator(this.tid);
        this.dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile, prefixed with the tableAlias string from the
     * constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.dbFileIterator != null && this.dbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return this.dbFileIterator.next();
    }

    public void close() {
        // some code goes here
        this.dbFileIterator.close();
        this.dbFileIterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.dbFileIterator.rewind();
    }
}
