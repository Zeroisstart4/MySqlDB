package simpledb.common;

import simpledb.storage.BufferPool;
import simpledb.storage.LogFile;

import java.io.*;
import java.util.concurrent.atomic.AtomicReference;


/**
 * @author zhou <br/>
 * Database ���ڳ�ʼ�����ݿ�ϵͳʹ�õļ�����̬�������� <br/>
 * ���ر���Ŀ¼������غ���־�ļ����� <br/>
 * �ṩһ������ڴ��κεط�������Щ�����ķ�����
 */
public class Database {

    /**
     * ���ݿ�ʵ����ʹ�� AtomicReference ��֤���õ��̰߳�ȫ
     */
    private static AtomicReference<Database> _instance = new AtomicReference<Database>(new Database());
    /**
     * database �����б�ļ���
     */
    private final Catalog _catalog;
    /**
     * פ�����ڴ����������ݿ��ļ�ҳ�ļ���
     */
    private final BufferPool _bufferpool;
    /**
     * ��־�ļ���
     */
    private final static String LOGFILENAME = "log";
    /**
     * ��־�ļ�
     */
    private final LogFile _logfile;

    /**
     * ���캯��
     */
    private Database() {
        _catalog = new Catalog();
        _bufferpool = new BufferPool(BufferPool.DEFAULT_PAGES);
        LogFile tmp = null;
        try {
            tmp = new LogFile(new File(LOGFILENAME));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        _logfile = tmp;
    }

    /**
     * ���ؾ�̬���ݿ�ʵ������־�ļ�
     * @return
     */
    public static LogFile getLogFile() {
        return _instance.get()._logfile;
    }

    /**
     * ���ؾ�̬���ݿ�ʵ���Ļ����
     * @return
     */
    public static BufferPool getBufferPool() {
        return _instance.get()._bufferpool;
    }

    /**
     * ���ؾ�̬���ݿ�ʵ����Ŀ¼
     * @return
     */
    public static Catalog getCatalog() {
        return _instance.get()._catalog;
    }

    /**
     * ���ڲ��Եķ�����������һ���µĻ����ʵ��������
     * @param pages     ҳ��
     * @return
     */
    public static BufferPool resetBufferPool(int pages) {
        // ������ƴ��������
        java.lang.reflect.Field bufferPoolF = null;
        try {
            bufferPoolF = Database.class.getDeclaredField("_bufferpool");
            bufferPoolF.setAccessible(true);
            bufferPoolF.set(_instance.get(), new BufferPool(pages));
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return _instance.get()._bufferpool;
    }

    /**
     * �������ݿ⣬�����ڵ�Ԫ���ԡ�
     */
    public static void reset() {
        _instance.set(new Database());
    }

}
