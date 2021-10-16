package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author zhou <br/>
 * 元组维护有关元组内容的信息。 <br/>
 * 元组具有由 TupleDesc 对象指定的指定约束，并包含带有每个字段数据的 Field 对象
 */
public class Tuple implements Serializable {

    /**
     * 序列化号
     */
    private static final long serialVersionUID = 1L;
    /**
     *描述元组的约束
     */
    private TupleDesc tupleDesc;
    /**
     * 记录 ID
     */
    private RecordId recordId;
    /**
     * 字段集合
     */
    private List<Field> fields;

    /**
     * 使用指定的约束（类型）创建一个新元组。
     * @param td
     */
    public Tuple(TupleDesc td) {
        this.tupleDesc = td;
        fields = new ArrayList<>();
        for (int i = 0; i < td.numFields(); i++) {
            fields.add(null);
        }
    }

    /**
     *返回此元组的约束
     * @return
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * 返回表示此元组在磁盘上的位置的 RecordId。 可能为空。
     * @return
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * 设置此元组的 RecordId 信息
     * @param rid   此元组的 RecordId
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * 更改此元组的第 i 个字段的值
     * @param i     索引
     * @param f     字段
     */
    public void setField(int i, Field f) {
        fields.set(i, f);
    }

    /**
     * 返回第 i 个字段的值，如果尚未设置，则为 null。
     * @param i     索引
     * @return
     */
    public Field getField(int i) {
        return fields.get(i);
    }

    /**
     * 以字符串形式返回此元组的内容。
     * 请注意，要通过系统测试，格式需要如下： column1\tcolumn2\tcolumn3\t...\tcolumnN 其中 \t 是任何空格（换行符除外）
     * @return
     */
    @Override
    public String toString() {
        StringBuilder sb =  new StringBuilder();
        for(int i = 0; i < tupleDesc.numFields(); ++i){
            sb.append(fields.get(i).toString()+" ");
        }
        return sb.toString();
    }

    /**
     * 返回遍历此元组的所有字段的迭代器
     * @return
     */
    public Iterator<Field> fields() {
        return fields.iterator();
    }

    /**
     * 重置此元组的 TupleDesc（仅影响 TupleDesc）
     * @param td
     */
    public void resetTupleDesc(TupleDesc td) {
        this.tupleDesc = td;
        // TODO: 是否需要重设fields?
    }
}
