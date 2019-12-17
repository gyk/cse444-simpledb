package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    File file;
    TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        if (tableId != this.getId()) {
            return null;
        }

        int pageSize = BufferPool.getPageSize();
        byte[] data = new byte[pageSize];
        int pageNo = pid.pageNumber();
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "r");
            raf.seek(pageSize * pageNo);
            raf.read(data);

            HeapPageId hpid = new HeapPageId(tableId, pid.pageNumber());
            return new HeapPage(hpid, data);
        } catch (IOException e) {
            // ignored
        }
        return null;

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            int iPage;
            int nPages;
            Iterator<Tuple> tupleIter;

            private void loadTupleIterator() throws DbException, TransactionAbortedException {
                PageId pid = new HeapPageId(getId(), this.iPage);
                Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                this.tupleIter = ((HeapPage) page).iterator();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.iPage = 0;
                this.nPages = numPages();
                loadTupleIterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return this.tupleIter != null &&
                        (this.tupleIter.hasNext() || this.iPage < this.nPages - 1);
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else if (this.tupleIter.hasNext()) {
                    return this.tupleIter.next();
                } else {
                    this.iPage++;
                    loadTupleIterator();
                    return this.tupleIter.next();
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                this.tupleIter = null;
            }
        };
    }

}

