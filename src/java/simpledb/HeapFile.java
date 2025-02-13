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
    int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
        this.numPages = (int) (this.file.length() / BufferPool.getPageSize());
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
            if (pageNo < numPages()) {
                try (RandomAccessFile raf = new RandomAccessFile(this.file, "r")) {
                    raf.seek(pageSize * pageNo);
                    raf.read(data);
                }
            }

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
        byte[] data = page.getPageData();
        int pageNo = page.getId().pageNumber();
        if (pageNo < numPages()) {
            int pageSize = BufferPool.getPageSize();
            try (RandomAccessFile raf = new RandomAccessFile(this.file, "rw")) {
                raf.seek(pageSize * pageNo);
                raf.write(data);
            }
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page = null;
        int numPages = numPages();
        int lastPageNo = numPages - 1;
        for (int i = 0; i < numPages; i++) {
            HeapPageId pid = new HeapPageId(getId(), lastPageNo);
            HeapPage candi = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (candi.getNumEmptySlots() > 0) {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                break;
            } else {
                Database.getBufferPool().releasePage(tid, pid);
            }
        }

        if (page == null) {
            int newPageNo = numPages;
            this.numPages++;
            HeapPageId pid = new HeapPageId(getId(), newPageNo);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        }

        page.insertTuple(t);
        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);

        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            int iPage;
            Iterator<Tuple> tupleIter;

            private void loadTupleIterator() throws DbException, TransactionAbortedException {
                PageId pid = new HeapPageId(getId(), this.iPage);
                Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                this.tupleIter = ((HeapPage) page).iterator();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.iPage = 0;
                loadTupleIterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (this.tupleIter == null) {
                    return false;
                } else if (this.tupleIter.hasNext()) {
                    return true;
                } else if (this.iPage < numPages() - 1) {
                    this.iPage++;
                    loadTupleIterator();
                    return this.tupleIter.hasNext();
                } else {
                    return false;
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return this.tupleIter.next();
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

