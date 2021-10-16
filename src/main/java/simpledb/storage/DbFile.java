
package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * @author zhou <br/>
 *
 * 磁盘上数据库文件的接口。                              <br/>
 * 每个表由单个 DbFile 表示。                           <br/>
 * DbFiles 可以获取页面并遍历元组。                      <br/>
 * 每个文件都有一个唯一的 id，用于存储关于目录中表的元数据。   <br/>
 * DbFiles 通常通过缓冲池访问，而不是由操作员直接访问        <br/>
 */
public interface DbFile {

    /**
     * 从磁盘读取指定页
     * @param id
     * @return
     */
    public Page readPage(PageId id);

    /**
     * 将指定页面推送到磁盘。
     * @param p
     * @throws IOException
     */
    public void writePage(Page p) throws IOException;

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException;

    /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to delete.  This tuple should be updated to reflect that
     *          it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException;

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    public DbFileIterator iterator(TransactionId tid);

    /**
     * Returns a unique ID used to identify this DbFile in the Catalog. This id
     * can be used to look up the table via {@link Catalog#getDatabaseFile} and
     * {@link Catalog#getTupleDesc}.
     * <p>
     * Implementation note:  you will need to generate this tableid somewhere,
     * ensure that each HeapFile has a "unique id," and that you always
     * return the same value for a particular HeapFile. A simple implementation
     * is to use the hash code of the absolute path of the file underlying
     * the HeapFile, i.e. <code>f.getAbsoluteFile().hashCode()</code>.
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId();
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc();
}
