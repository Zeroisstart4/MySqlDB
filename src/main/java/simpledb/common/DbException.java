package simpledb.common;

import java.lang.Exception;


/**
 * @author zhou <br/>
 * 通用数据库异常类
 */
public class DbException extends Exception {
    private static final long serialVersionUID = 1L;

    public DbException(String s) {
        super(s);
    }
}
