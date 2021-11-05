package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;


/**
 * @author zhou <br/>
 *����Ԫ���Լ��
 *
 * ���±� student  <br/>
 *  _______________________________________________  <br/>
 * |    id(int)  |  name(string)    |sex(string)   | <br/>
 * |    1        |   xxx            |   m          | <br/>
 * |    2        |   yyy            |   f          | <br/>
 * |_______________________________________________| <br/>
 *
 * ��ô (1, xxx, m) ����һ�� Tuple�� TupleDesc ���� (id(int) name(string) sex(string))��
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
     * ��ȡ�ܵ��ֶ����ͳ���
     * @return
     */
    public int getSize() {
        int sz = 0;
        for (TDItem item : items) {
            sz += item.fieldType.getLen();
        }
        return sz;
    }


    /**
     * ������ TupleDescs �ϲ�Ϊһ�������� td1.numFields + td2.numFields �ֶΣ���һ�� td1.numFields ���� td1���������� td2��
     * @param td1   ������ TupleDesc �ĵ�һ���ֶε� TupleDesc
     * @param td2   �� TupleDesc ��ʣ���ֶ�
     * @return
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
        return new TupleDesc(types.toArray(new Type[0]), names.toArray(new String[0]));
    }

    /**
     * �Ƚ�ָ��������� TupleDesc �Ƿ���ȡ�<br/>
     * ������� TupleDesc ������ͬ��������Ŀ�����Ҷ���ÿ�� i������� TupleDesc �еĵ� i �����͵��� o �еĵ� i �����ͣ�����Ϊ������ȡ�
     * @param o
     * @return
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
