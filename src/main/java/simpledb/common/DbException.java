package simpledb.common;

import java.lang.Exception;


/**
 * @author zhou <br/>
 * ͨ�����ݿ��쳣��
 */
public class DbException extends Exception {
    private static final long serialVersionUID = 1L;

    public DbException(String s) {
        super(s);
    }
}
