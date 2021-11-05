package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author zhou <br/>
 * Ԫ��ά���й�Ԫ�����ݵ���Ϣ�� <br/>
 * Ԫ������� TupleDesc ����ָ����ָ��Լ��������������ÿ���ֶ����ݵ� Field ����
 */
public class Tuple implements Serializable {

    /**
     * ���л���
     */
    private static final long serialVersionUID = 1L;
    /**
     *����Ԫ���Լ��
     */
    private TupleDesc tupleDesc;
    /**
     * ��¼ ID
     */
    private RecordId recordId;
    /**
     * �ֶμ���
     */
    private List<Field> fields;

    /**
     * ʹ��ָ����Լ�������ͣ�����һ����Ԫ�顣
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
     *���ش�Ԫ���Լ��
     * @return
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * ���ر�ʾ��Ԫ���ڴ����ϵ�λ�õ� RecordId�� ����Ϊ�ա�
     * @return
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * ���ô�Ԫ��� RecordId ��Ϣ
     * @param rid   ��Ԫ��� RecordId
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * ���Ĵ�Ԫ��ĵ� i ���ֶε�ֵ
     * @param i     ����
     * @param f     �ֶ�
     */
    public void setField(int i, Field f) {
        fields.set(i, f);
    }

    /**
     * ���ص� i ���ֶε�ֵ�������δ���ã���Ϊ null��
     * @param i     ����
     * @return
     */
    public Field getField(int i) {
        return fields.get(i);
    }

    /**
     * ���ַ�����ʽ���ش�Ԫ������ݡ�
     * ��ע�⣬Ҫͨ��ϵͳ���ԣ���ʽ��Ҫ���£� column1\tcolumn2\tcolumn3\t...\tcolumnN ���� \t ���κοո񣨻��з����⣩
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
     * ���ر�����Ԫ��������ֶεĵ�����
     * @return
     */
    public Iterator<Field> fields() {
        return fields.iterator();
    }

    /**
     * ���ô�Ԫ��� TupleDesc����Ӱ�� TupleDesc��
     * @param td
     */
    public void resetTupleDesc(TupleDesc td) {
        this.tupleDesc = td;
        // TODO: �Ƿ���Ҫ����fields?
    }
}
