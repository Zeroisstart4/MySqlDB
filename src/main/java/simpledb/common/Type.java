package simpledb.common;

import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;

import java.text.ParseException;
import java.io.*;


/**
 * @author zhou <br/>
 *
 * ��ʾ SimpleDB �е����͵��ࡣ �����Ǵ��ඨ��ľ�̬���� ��ˣ�Type ���캯����˽�еġ�
 */
public enum Type implements Serializable {

    /**
     * int ���ͣ� ��СΪ 4 �ֽ�
     */
    INT_TYPE() {
        @Override
        public int getLen() {
            return 4;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                return new IntField(dis.readInt());
            }  catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }

    },
    /**
     * String ���ͣ� ��СΪ STRING_LEN + 4 �ֽ�
     */
    STRING_TYPE() {
        @Override
        public int getLen() {
            return STRING_LEN + 4;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                int strLen = dis.readInt();
                byte bs[] = new byte[strLen];
                dis.read(bs);
                dis.skipBytes(STRING_LEN - strLen);
                return new StringField(new String(bs), STRING_LEN);
            } catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }
    };

    public static final int STRING_LEN = 128;

    /**
     * �洢�������͵��ֶ�������ֽ���
     * @return
     */
    public abstract int getLen();

    /**
     * ������˶��������ͬ���͵� Field ���󣬸ö�����д�ָ���� DataInputStream ��ȡ�����ݡ�
     * @param dis
     * @return
     * @throws ParseException
     */
    public abstract Field parse(DataInputStream dis) throws ParseException;

}
