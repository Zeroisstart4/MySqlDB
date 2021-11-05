package simpledb.storage;

import simpledb.excution.Predicate;
import simpledb.common.Type;

import java.io.*;


/**
 * @author zhou <br/>
 *
 * SimpleDB Ԫ���е��ֶνӿ�
 */
public interface Field extends Serializable{
    /**
     * ���л�
     * @param dos
     * @throws IOException
     */
    void serialize(DataOutputStream dos) throws IOException;

    /**
     * �����ֶζ����ֵ�봫���ֵ���бȽ�
     * @param op        ������
     * @param value     �ֶ�ֵ
     * @return
     */
    public boolean compare(Predicate.Op op, Field value);

    /**
     * ���ش��ֶε�����
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
