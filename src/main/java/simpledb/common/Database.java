package simpledb.common;

import simpledb.storage.BufferPool;
import simpledb.storage.LogFile;

import java.io.*;
import java.util.concurrent.atomic.AtomicReference;


/**
 * @author zhou <br/>
 * Database 用于初始化数据库系统使用的几个静态变量的类 <br/>
 * （特别是目录、缓冲池和日志文件。） <br/>
 * 提供一组可用于从任何地方访问这些变量的方法。
 */
public class Database {

    /**
     * 数据库实例，使用 AtomicReference 保证引用的线程安全
     */
    private static AtomicReference<Database> _instance = new AtomicReference<Database>(new Database());
    /**
     * database 中所有表的集合
     */
    private final Catalog _catalog;
    /**
     * 驻留在内存中所有数据库文件页的集合
     */
    private final BufferPool _bufferpool;
    /**
     * 日志文件名
     */
    private final static String LOGFILENAME = "log";
    /**
     * 日志文件
     */
    private final LogFile _logfile;

    /**
     * 构造函数
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
     * 返回静态数据库实例的日志文件
     * @return
     */
    public static LogFile getLogFile() {
        return _instance.get()._logfile;
    }

    /**
     * 返回静态数据库实例的缓冲池
     * @return
     */
    public static BufferPool getBufferPool() {
        return _instance.get()._bufferpool;
    }

    /**
     * 返回静态数据库实例的目录
     * @return
     */
    public static Catalog getCatalog() {
        return _instance.get()._catalog;
    }

    /**
     * 用于测试的方法——创建一个新的缓冲池实例并返回
     * @param pages     页数
     * @return
     */
    public static BufferPool resetBufferPool(int pages) {
        // 反射机制创建缓冲池
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
     * 重置数据库，仅用于单元测试。
     */
    public static void reset() {
        _instance.set(new Database());
    }

}
