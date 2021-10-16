package simpledb.common;

import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;

import java.text.ParseException;
import java.io.*;


/**
 * @author zhou <br/>
 *
 * 表示 SimpleDB 中的类型的类。 类型是此类定义的静态对象； 因此，Type 构造函数是私有的。
 */
public enum Type implements Serializable {

    /**
     * int 类型， 大小为 4 字节
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
     * String 类型， 大小为 STRING_LEN + 4 字节
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
     * 存储这种类型的字段所需的字节数
     * @return
     */
    public abstract int getLen();

    /**
     * 返回与此对象具有相同类型的 Field 对象，该对象具有从指定的 DataInputStream 读取的内容。
     * @param dis
     * @return
     * @throws ParseException
     */
    public abstract Field parse(DataInputStream dis) throws ParseException;

}
