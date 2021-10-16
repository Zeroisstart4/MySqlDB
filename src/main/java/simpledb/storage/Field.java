package simpledb.storage;

import simpledb.excution.Predicate;
import simpledb.common.Type;

import java.io.*;


/**
 * @author zhou <br/>
 *
 * SimpleDB 元组中的字段接口
 */
public interface Field extends Serializable{
    /**
     * 序列化
     * @param dos
     * @throws IOException
     */
    void serialize(DataOutputStream dos) throws IOException;

    /**
     * 将此字段对象的值与传入的值进行比较
     * @param op        操作符
     * @param value     字段值
     * @return
     */
    public boolean compare(Predicate.Op op, Field value);

    /**
     * 返回此字段的类型
     * @return
     */
    public Type getType();
    
    /**
     * Hash code.
     * Different Field objects representing the same value should probably
     * return the same hashCode.
     */
    @Override
    public int hashCode();

    @Override
    public boolean equals(Object field);

    @Override
    public String toString();
}
