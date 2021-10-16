package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;


/**
 * @author zhou <br/>
 *描述元组的约束
 *
 * 如下表 student  <br/>
 *  _______________________________________________  <br/>
 * |    id(int)  |  name(string)    |sex(string)   | <br/>
 * |    1        |   xxx            |   m          | <br/>
 * |    2        |   yyy            |   f          | <br/>
 * |_______________________________________________| <br/>
 *
 * 那么 (1, xxx, m) 就是一个 Tuple， TupleDesc 则是 (id(int) name(string) sex(string))。
 */
public class TupleDesc implements Serializable {

    /**
     * TDItem 集合类
     */
    List<TDItem> items;

    /**
     * 一个帮助类，便于组织各个领域的信息
     */
    public static class TDItem implements Serializable {

        /**
         * 序列化号
         */
        private static final long serialVersionUID = 1L;
        /**
         * 字段类型
         */
        public final Type fieldType;
        /**
         * 字段名称
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
     * TDItem 集合类的迭代器
     * @return
     */
    public Iterator<TDItem> iterator() {
        return items.iterator();
    }

    /**
     * 序列化号
     */
    private static final long serialVersionUID = 1L;


    /**
     * 创建一个 TupleDesc ，其中包含指定类型的字段，以及关联的命名字段
     * @param typeAr    字段类型数组
     * @param fieldAr   字段名称数组
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        items = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            items.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * 创建一个 TupleDesc，其中包含指定类型的字段，以及匿名（未命名）字段。
     * @param typeAr    字段类型数组
     */
    public TupleDesc(Type[] typeAr) {
        items = new ArrayList<>();
        for (Type type : typeAr) {
            items.add(new TDItem(type, ""));
        }
    }

    /**
     * 返回 TupleDesc 中的字段数
     * @return
     */
    public int numFields() {
        return items.size();
    }

    /**
     * 获取此 TupleDesc 的第 i 个字段的（可能为空）字段名称
     * @param i     索引
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
     * 获取 TupleDesc 的第 i 个字段的类型
     * @param i     索引
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
     * 查找具有给定名称的字段的索引
     * @param name      字段名称
     * @return
     * @throws NoSuchElementException
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        String[] tmp = name.split("\\.");
        name = tmp[tmp.length - 1];
        // 上面这两行是为了执行 catalog.txt... 否则会报错？？
        for (int i = 0; i < items.size(); i++) {
            if(items.get(i).fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * 获取总的字段类型长度
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
     * 将两个 TupleDescs 合并为一个，具有 td1.numFields + td2.numFields 字段，第一个 td1.numFields 来自 td1，其余来自 td2。
     * @param td1   带有新 TupleDesc 的第一个字段的 TupleDesc
     * @param td2   新 TupleDesc 的剩余字段
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
     * 比较指定对象与此 TupleDesc 是否相等。<br/>
     * 如果两个 TupleDesc 具有相同数量的项目，并且对于每个 i，如果此 TupleDesc 中的第 i 个类型等于 o 中的第 i 个类型，则认为它们相等。
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
        // 未进行实现
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * 返回描述此描述符的字符串。 <br/>
     * 形式如 “fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])”
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
