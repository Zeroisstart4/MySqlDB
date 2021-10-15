package simpledb;

import java.io.Serializable;
import java.util.*;


/**
 * @author zhou <br/>
 *����Ԫ���Լ��
 */
public class TupleDesc implements Serializable {

    /**
     * TDItem ������
     */
    List<TDItem> items;

    /**
     * һ�������࣬������֯�����������Ϣ
     */
    public static class TDItem implements Serializable {

        /**
         * ���л���
         */
        private static final long serialVersionUID = 1L;
        /**
         * �ֶ�����
         */
        public final Type fieldType;
        /**
         * �ֶ�����
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * TDItem ������ĵ�����
     * @return
     */
    public Iterator<TDItem> iterator() {
        return items.iterator();
    }

    /**
     * ���л���
     */
    private static final long serialVersionUID = 1L;


    /**
     * ����һ�� TupleDesc �����а���ָ�����͵��ֶΣ��Լ������������ֶ�
     * @param typeAr    �ֶ���������
     * @param fieldAr   �ֶ���������
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        items = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            items.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * ����һ�� TupleDesc�����а���ָ�����͵��ֶΣ��Լ�������δ�������ֶΡ�
     * @param typeAr    �ֶ���������
     */
    public TupleDesc(Type[] typeAr) {
        items = new ArrayList<>();
        for (Type type : typeAr) {
            items.add(new TDItem(type, ""));
        }
    }

    /**
     * ���� TupleDesc �е��ֶ���
     * @return
     */
    public int numFields() {
        return items.size();
    }

    /**
     * ��ȡ�� TupleDesc �ĵ� i ���ֶεģ�����Ϊ�գ��ֶ�����
     * @param i     ����
     * @return
     * @throws NoSuchElementException
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i >= numFields()||i < 0) {
            throw new NoSuchElementException();
        }
        else {
            return items.get(i).fieldName;
        }
    }

    /**
     * ��ȡ TupleDesc �ĵ� i ���ֶε�����
     * @param i     ����
     * @return
     * @throws NoSuchElementException
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i >= numFields()||i < 0) {
            throw new NoSuchElementException();
        }
        else {
            return items.get(i).fieldType;
        }
    }

    /**
     * ���Ҿ��и������Ƶ��ֶε�����
     * @param name      �ֶ�����
     * @return
     * @throws NoSuchElementException
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        String[] tmp = name.split("\\.");
        name = tmp[tmp.length - 1];
        // ������������Ϊ��ִ�� catalog.txt... ����ᱨ����
        for (int i = 0; i < items.size(); i++) {
            if(items.get(i).fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int sz = 0;
        for (TDItem item : items) {
            sz += item.fieldType.getLen();
        }
        return sz;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        ArrayList<Type> types = new ArrayList<>();
        ArrayList<String> names = new ArrayList<>();
        Iterator<TDItem> iterator = td1.iterator();
        while (iterator.hasNext()){
            TDItem tmp = iterator.next();
            types.add(tmp.fieldType);
            names.add(tmp.fieldName);
        }
        iterator = td2.iterator();
        while (iterator.hasNext()){
            TDItem tmp = iterator.next();
            types.add(tmp.fieldType);
            names.add(tmp.fieldName);
        }
        return new TupleDesc(types.toArray(new Type[0]),names.toArray(new String[0]));
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    @Override
    public boolean equals(Object o) {
//        same number of items
//        i-th type in this TupleDesc is equal to the i-th type in o for every i.
        boolean res = false;
        if (o instanceof TupleDesc){
            TupleDesc td = (TupleDesc)o;
            if(td.numFields()==numFields()){
                res = true;
                for (int i = 0; i < numFields(); i++) {
                    if(!getFieldType(i).equals(td.getFieldType(i))){
                        res = false;
                        break;
                    }
                }
            }
        }
        return res;
    }

    @Override
    public int hashCode() {
        // δ����ʵ��
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * �������������������ַ����� <br/>
     * ��ʽ�� ��fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])��
     * @return
     */
    @Override
    public String toString() {
        if(items.size()==0){
            return "";
        }else{
            StringBuffer sb = new StringBuffer(items.get(0).toString());
            for (int i = 1; i < items.size(); i++) {
                sb.append(",");
                sb.append(items.get(i).toString());
            }
            return sb.toString();
        }
    }
}
