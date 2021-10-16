package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhou
 *
 * BufferPool 功能                <br/>
 * 1. 管理页面从磁盘读取和写入内存。      <br/>
 * 2. 检索页面，并从适当的位置获取页面。<br/>
 * 3. 负责事务锁机制的实现； 当一个事务获取一个页面时，BufferPool 会检查该事务是否有合适的锁来读/写页面。<br/>
 */
public class BufferPool {

    /**
     * 每页的字节数 = 4096 byte 即 4 K，包括标题
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    /**
     * 页面大小默认为 4 K
     */
    private static int pageSize = DEFAULT_PAGE_SIZE;
    /**
     * 默认最大页数
     */
    public static final int DEFAULT_PAGES = 50;
    /**
     * 缓冲池中的最大页面数
     */
    private int numPages;
    /**
     * 缓冲池中的页面，通过页面号获取对应页面
     */
    private ConcurrentHashMap<Integer, Page> pages;
    /**
     * 锁管理器，用于事务锁管理
     */
    private LockManager lockManager;

    /**
     * 创建一个缓存最多为 numPages 个页面的 BufferPool
     * @param numPages  缓冲池中的最大页面数
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        pages = new ConcurrentHashMap<>();
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    private Integer getKey(PageId pageId){
        return pageId.hashCode(); // 使用hashcode来作为key
    }

    /**
     * 检索具有关联权限的指定页面。 将获取一个锁，如果该锁被另一个事务持有，则会阻塞  <br/>
     * 在缓冲池中查找检索到的页面，可能结果如下：                    <br/>
     * 1. 如果存在，则将其返回。                                  <br/>
     * 2. 如果不存在，则将其添加到缓冲池并返回。                     <br/>
     * 3. 如果缓冲池中的空间不足，则逐出页面并在逐出页位置添加新页面。   <br/>
     * @param tid       请求页面的事务的 ID
     * @param pid       请求页面的 ID
     * @param perm      页面请求的权限 (READ_ONLY /READ_WRITE)
     * @return
     * @throws TransactionAbortedException
     * @throws DbException
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        Object lock = lockManager.getTxLock(pid);
        synchronized (lock){
            LockType lockType;
            if(perm == Permissions.READ_ONLY){
                lockType = LockType.SHARED_LOCK;
            }else{
                lockType = LockType.EXCLUSIVE_LOCK;
            }
            boolean lockStatus = lockManager.acquireLock(pid, new Lock(tid, lockType));
            if(!lockStatus){
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            Integer key = getKey(pid);
            if(!pages.containsKey(key)){
                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                while (pages.size() >= numPages){
                    evictPage();
                }
                pages.put(key, dbFile.readPage(pid));
            }
            return pages.get(key);
        }
    }

    /**
     * 释放页面锁
     * @param tid      事务 ID
     * @param pid      页面 ID
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(pid,tid);
    }

    /**
     * 事务执行完毕后，释放与给定事务关联的所有锁
     * @param tid       事务 ID
     * @throws IOException
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /**
     * 检测指定事务在指定页面上是否有锁
     * @param tid       事务 ID
     * @param p         页面 ID
     * @return
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(p,tid);
    }

    /**
     * 提交或中止给定的事务； 释放与事务关联的所有锁
     * @param tid       事务 ID
     * @param commit    是否提交事务
     * @throws IOException
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit){
            flushPages(tid);
        }else{
            // abort page
        }
        lockManager.releaseLock(tid);
    }

    /**
     * 事务 tid 向指定表添加元组。 <br/>
     * 将在添加元组的页面和任何其他更新的页面上获取写锁（lab2 不需要获取锁）。 <br/>
     * 如果无法获取锁，则可能会阻塞。 <br/>
     * 通过调用其 markDirty 将任何被操作过的页面标记为脏页面，<br/>
     * 并将脏页面的版本添加到缓存中（替换这些页面的现有版本），以便将来的请求看到最新的页面.
     * @param tid           事务 ID
     * @param tableId       表 ID
     * @param t             元组
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pages =  Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
//        for(Page page:pages){
//            page.markDirty(true,tid);
//        }
        updateBufferPool(tid,pages);
    }

    /**
     * 从缓冲池中删除指定的元组。 <br/>
     * 在删除元组的页面和更新的任何其他页面上获取写锁。 <br/>
     * 如果无法获取锁，则可能会阻塞。 <br/>
     * 通过调用其 markDirty 位将任何被操作过的页面标记为脏页面，<br/>
     * 并将脏页面的版本添加到缓存中（替换这些页面的任何现有版本），以便将来的请求看到最新的页面.<br/>
     * @param tid       事务 ID
     * @param t         元组
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pages =  Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
//        for(Page page:pages){
//            page.markDirty(true,tid);
//        }
        updateBufferPool(tid, pages);
    }

    /**
     * 1. 标记脏页
     * 2. 替换这些页面的现有版本
     * @param tid       事务 ID
     * @param pages     页面集合
     */
    private void updateBufferPool(TransactionId tid, List<Page> pages){
        for (Page page : pages){
            // make dirty
            page.markDirty(true, tid);
            int key = getKey(page.getId());
            if(!this.pages.containsKey(key)) {
                while (this.pages.size() >= numPages) {
                    try {
                        evictPage();
                    } catch (DbException e) {
                        e.printStackTrace();
                    }
                }
            }
            this.pages.put(key, page);
        }
    }

    /**
     * 将所有脏页刷新到磁盘
     * @throws IOException
     */
    public synchronized void flushAllPages() throws IOException {
        for(Page page : pages.values()){
            flushPage(page.getId());
        }
    }

    /**
     * 从缓冲池中删除指定的页面
     * @param pid
     */
    public synchronized void discardPage(PageId pid) {
        int key = getKey(pid);
        pages.remove(key);
    }

    /**
     * 将指定页面刷新到磁盘，并将写入磁盘页面标记为非脏页，同时将其保留在 BufferPool 中
     * @param pid
     * @throws IOException
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = pages.get(getKey(pid));
        if(page.isDirty() != null){
            DbFile dbFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
            dbFile.writePage(page);
            page.markDirty(false,null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * 从缓冲池中逐出一个页面。 将页面刷新到磁盘以确保脏页面在磁盘上更新
     * @throws DbException
     */
    private synchronized void evictPage() throws DbException {
        // 采用最简单的随机淘汰策略
        List<Integer> keys = new ArrayList<>(pages.keySet());
        int randomKey = keys.get(new Random().nextInt(keys.size()));
        PageId evictPid = pages.get(randomKey).getId();
        try {
            flushPage(evictPid);
        } catch (IOException e) {
            e.printStackTrace();
        }
        discardPage(evictPid);
    }


    /**
     * 锁类型枚举类，分为共享锁与排他锁两大类 <br/>
     * 1. SHARED_LOCK               <br/>
     * 2. EXCLUSIVE_LOCK            <br/>
     */
    enum LockType {
        SHARED_LOCK, EXCLUSIVE_LOCK;
    }

    /**
     * 锁信息类，用于记录锁相关信息
     */
    private class Lock {

        /**
         * 事务 ID
         */
        TransactionId tid;
        /**
         * 锁类型：0 表示共享锁，1 表示排他锁
         */
        LockType lockType;

        public Lock(TransactionId tid, LockType lockType){
            this.tid = tid;
            this.lockType = lockType;
        }
    }

    /**
     * 锁管理器类
     */
    private class LockManager {

        /**
         * 页面锁
         */
        ConcurrentHashMap<PageId, Vector<Lock>> pageLocks = new ConcurrentHashMap<>();
        /**
         * 事务锁
         */
        ConcurrentHashMap<PageId, Object> txLocks = new ConcurrentHashMap<>();

        /**
         * 获取锁 <br/>
         * 场景：已持有锁、尝试获取读锁、尝试获取写锁、读锁升级写锁 <br/>
         * 注意：别的事务持有锁，导致当前事务无法获取锁之后需要进行等待和通知    <br/>
         * @return 是否获取锁成功
         */
        public synchronized boolean acquireLock(PageId pageId,Lock lock){
            if(pageLocks.get(pageId) == null){ // 没人有锁，成功
                Vector<Lock> locks = new Vector<>();
                locks.add(lock);
                pageLocks.put(pageId, locks);
                return true;
            }
            Vector<Lock> locks = pageLocks.get(pageId);
            for(Lock l : locks) {
                if(l.tid.equals(lock.tid)) {
                    if(l.lockType == lock.lockType) {
                        return true;  // 已持有锁
                    }
                    else {
                        if(l.lockType == LockType.EXCLUSIVE_LOCK){
                            return true; // 已有排它锁，可以有共享锁
                        }
                        else if(l.lockType == LockType.SHARED_LOCK){
                            if(locks.size() == 1){
                                // 读锁升级写锁
                                l.lockType = LockType.EXCLUSIVE_LOCK;
                                return true;
                            }
                            else {
                                return false;
                            }
                        }

                    }
                }
            }
            // 如果没有过，并且其他都是共享锁，可以加锁
            if(lock.lockType == LockType.SHARED_LOCK && locks.get(0).lockType== LockType.SHARED_LOCK){
                locks.add(lock);
                return true;
            }
            return false;
        }

        /**
         * 释放锁
         * @return true: 成功 false: 没锁
         */
        public synchronized boolean releaseLock(TransactionId tid){
            for(PageId pageId : pageLocks.keySet()){
                if(holdsLock(pageId, tid)){
                    boolean res = releaseLock(pageId, tid);
                    if(!res){
                        return false;
                    }
                    // TODO：先全部唤醒，可优化
                    Object object = getTxLock(pageId);
                    object.notifyAll();
                }
            }
            return true;
        }

        /**
         * 释放锁
         * @return true: 成功 false: 没锁
         */
        public synchronized boolean releaseLock(PageId pageId, TransactionId tid){
            assert pageLocks.get(pageId) != null : "page not locked!";
            Vector<Lock> locks = pageLocks.get(pageId);
            for(Lock l : locks){
                if(l.tid.equals(tid)){
                    locks.remove(l);
                    if(locks.size() == 0){
                        pageLocks.remove(pageId);
                    }
                    return true;
                }
            }
            return false;
        }

        /**
         * 判断tix是否持有锁
         */
        public synchronized boolean holdsLock(PageId pageId, TransactionId tid){
            Vector<Lock> locks = pageLocks.get(pageId);
            if(locks==null){
                return false;
            }
            for(Lock l : locks){
                if(l.tid.equals(tid)){
                    return true;
                }
            }
            return false;
        }

        public Object getTxLock(PageId pageId){
            txLocks.computeIfAbsent(pageId, k -> new Object());
            return txLocks.get(pageId);
        }
    }

}
