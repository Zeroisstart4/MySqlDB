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
 * BufferPool ����                <br/>
 * 1. ����ҳ��Ӵ��̶�ȡ��д���ڴ档      <br/>
 * 2. ����ҳ�棬�����ʵ���λ�û�ȡҳ�档<br/>
 * 3. �������������Ƶ�ʵ�֣� ��һ�������ȡһ��ҳ��ʱ��BufferPool ����������Ƿ��к��ʵ�������/дҳ�档<br/>
 */
public class BufferPool {

    /**
     * ÿҳ���ֽ��� = 4096 byte �� 4 K����������
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    /**
     * ҳ���СĬ��Ϊ 4 K
     */
    private static int pageSize = DEFAULT_PAGE_SIZE;
    /**
     * Ĭ�����ҳ��
     */
    public static final int DEFAULT_PAGES = 50;
    /**
     * ������е����ҳ����
     */
    private int numPages;
    /**
     * ������е�ҳ�棬ͨ��ҳ��Ż�ȡ��Ӧҳ��
     */
    private ConcurrentHashMap<Integer, Page> pages;
    /**
     * ������������������������
     */
    private LockManager lockManager;

    /**
     * ����һ���������Ϊ numPages ��ҳ��� BufferPool
     * @param numPages  ������е����ҳ����
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
        return pageId.hashCode(); // ʹ��hashcode����Ϊkey
    }

    /**
     * �������й���Ȩ�޵�ָ��ҳ�档 ����ȡһ�����������������һ��������У��������  <br/>
     * �ڻ�����в��Ҽ�������ҳ�棬���ܽ�����£�                    <br/>
     * 1. ������ڣ����䷵�ء�                                  <br/>
     * 2. ��������ڣ�������ӵ�����ز����ء�                     <br/>
     * 3. ���������еĿռ䲻�㣬�����ҳ�沢�����ҳλ�������ҳ�档   <br/>
     * @param tid       ����ҳ�������� ID
     * @param pid       ����ҳ��� ID
     * @param perm      ҳ�������Ȩ�� (READ_ONLY /READ_WRITE)
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
     * �ͷ�ҳ����
     * @param tid      ���� ID
     * @param pid      ҳ�� ID
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(pid,tid);
    }

    /**
     * ����ִ����Ϻ��ͷ���������������������
     * @param tid       ���� ID
     * @throws IOException
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /**
     * ���ָ��������ָ��ҳ�����Ƿ�����
     * @param tid       ���� ID
     * @param p         ҳ�� ID
     * @return
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(p,tid);
    }

    /**
     * �ύ����ֹ���������� �ͷ������������������
     * @param tid       ���� ID
     * @param commit    �Ƿ��ύ����
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
     * ���� tid ��ָ�������Ԫ�顣 <br/>
     * �������Ԫ���ҳ����κ��������µ�ҳ���ϻ�ȡд����lab2 ����Ҫ��ȡ������ <br/>
     * ����޷���ȡ��������ܻ������� <br/>
     * ͨ�������� markDirty ���κα���������ҳ����Ϊ��ҳ�棬<br/>
     * ������ҳ��İ汾��ӵ������У��滻��Щҳ������а汾�����Ա㽫�������󿴵����µ�ҳ��.
     * @param tid           ���� ID
     * @param tableId       �� ID
     * @param t             Ԫ��
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
     * �ӻ������ɾ��ָ����Ԫ�顣 <br/>
     * ��ɾ��Ԫ���ҳ��͸��µ��κ�����ҳ���ϻ�ȡд���� <br/>
     * ����޷���ȡ��������ܻ������� <br/>
     * ͨ�������� markDirty λ���κα���������ҳ����Ϊ��ҳ�棬<br/>
     * ������ҳ��İ汾��ӵ������У��滻��Щҳ����κ����а汾�����Ա㽫�������󿴵����µ�ҳ��.<br/>
     * @param tid       ���� ID
     * @param t         Ԫ��
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
     * 1. �����ҳ
     * 2. �滻��Щҳ������а汾
     * @param tid       ���� ID
     * @param pages     ҳ�漯��
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
     * ��������ҳˢ�µ�����
     * @throws IOException
     */
    public synchronized void flushAllPages() throws IOException {
        for(Page page : pages.values()){
            flushPage(page.getId());
        }
    }

    /**
     * �ӻ������ɾ��ָ����ҳ��
     * @param pid
     */
    public synchronized void discardPage(PageId pid) {
        int key = getKey(pid);
        pages.remove(key);
    }

    /**
     * ��ָ��ҳ��ˢ�µ����̣�����д�����ҳ����Ϊ����ҳ��ͬʱ���䱣���� BufferPool ��
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
     * �ӻ���������һ��ҳ�档 ��ҳ��ˢ�µ�������ȷ����ҳ���ڴ����ϸ���
     * @throws DbException
     */
    private synchronized void evictPage() throws DbException {
        // ������򵥵������̭����
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
     * ������ö���࣬��Ϊ�������������������� <br/>
     * 1. SHARED_LOCK               <br/>
     * 2. EXCLUSIVE_LOCK            <br/>
     */
    enum LockType {
        SHARED_LOCK, EXCLUSIVE_LOCK;
    }

    /**
     * ����Ϣ�࣬���ڼ�¼�������Ϣ
     */
    private class Lock {

        /**
         * ���� ID
         */
        TransactionId tid;
        /**
         * �����ͣ�0 ��ʾ��������1 ��ʾ������
         */
        LockType lockType;

        public Lock(TransactionId tid, LockType lockType){
            this.tid = tid;
            this.lockType = lockType;
        }
    }

    /**
     * ����������
     */
    private class LockManager {

        /**
         * ҳ����
         */
        ConcurrentHashMap<PageId, Vector<Lock>> pageLocks = new ConcurrentHashMap<>();
        /**
         * ������
         */
        ConcurrentHashMap<PageId, Object> txLocks = new ConcurrentHashMap<>();

        /**
         * ��ȡ�� <br/>
         * �������ѳ����������Ի�ȡ���������Ի�ȡд������������д�� <br/>
         * ע�⣺�����������������µ�ǰ�����޷���ȡ��֮����Ҫ���еȴ���֪ͨ    <br/>
         * @return �Ƿ��ȡ���ɹ�
         */
        public synchronized boolean acquireLock(PageId pageId,Lock lock){
            if(pageLocks.get(pageId) == null){ // û���������ɹ�
                Vector<Lock> locks = new Vector<>();
                locks.add(lock);
                pageLocks.put(pageId, locks);
                return true;
            }
            Vector<Lock> locks = pageLocks.get(pageId);
            for(Lock l : locks) {
                if(l.tid.equals(lock.tid)) {
                    if(l.lockType == lock.lockType) {
                        return true;  // �ѳ�����
                    }
                    else {
                        if(l.lockType == LockType.EXCLUSIVE_LOCK){
                            return true; // �����������������й�����
                        }
                        else if(l.lockType == LockType.SHARED_LOCK){
                            if(locks.size() == 1){
                                // ��������д��
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
            // ���û�й��������������ǹ����������Լ���
            if(lock.lockType == LockType.SHARED_LOCK && locks.get(0).lockType== LockType.SHARED_LOCK){
                locks.add(lock);
                return true;
            }
            return false;
        }

        /**
         * �ͷ���
         * @return true: �ɹ� false: û��
         */
        public synchronized boolean releaseLock(TransactionId tid){
            for(PageId pageId : pageLocks.keySet()){
                if(holdsLock(pageId, tid)){
                    boolean res = releaseLock(pageId, tid);
                    if(!res){
                        return false;
                    }
                    // TODO����ȫ�����ѣ����Ż�
                    Object object = getTxLock(pageId);
                    object.notifyAll();
                }
            }
            return true;
        }

        /**
         * �ͷ���
         * @return true: �ɹ� false: û��
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
         * �ж�tix�Ƿ������
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
