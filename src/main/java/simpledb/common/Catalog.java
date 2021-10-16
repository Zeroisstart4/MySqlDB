package simpledb.common;

import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author zhou <br/>
 *
 * Catalog 会跟踪数据库中所有可用的表及其关联的模式。 <br/>
 * 目前，这是一个存根目录，在使用之前必须由用户程序用表填充 <br/>
 * 最终，这应该转换为从磁盘读取目录表的目录。
 */
public class Catalog {

    /**
     * 数据库表结构
     */
    class Table {
        /**
         * 表文件
         */
        private DbFile file;
        /**
         * 表名
         */
        private String name;
        /**
         * 主键
         */
        private String pkeyField;


        public Table(DbFile file, String name, String pkeyField){
            this.file = file;
            this.name = name;
            this.pkeyField = pkeyField;
        }

        public DbFile getFile() {
            return file;
        }

        public void setFile(DbFile file) {
            this.file = file;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getPkeyField() {
            return pkeyField;
        }

        public void setPkeyField(String pkeyField) {
            this.pkeyField = pkeyField;
        }
    }

    /**
     * 形如 (name,Table) 的串表 map
     */
    private ConcurrentHashMap<String,Table> stringTableMap;

    /**
     * 形如 (tableId,Table) 整形表 map
     */
    private ConcurrentHashMap<Integer,Table> integerTableMap;

    /**
     * 构造函数 <br/>
     * 创建一个新的空目录
     */
    public Catalog() {
        stringTableMap = new ConcurrentHashMap<>();
        integerTableMap = new ConcurrentHashMap<>();
    }

    /**
     * 向目录中添加一个新表。 该表的内容存储在指定的 DbFile 中
     * @param file          表文件
     * @param name          表名
     * @param pkeyField     主键字段
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        if(stringTableMap.containsKey(name)) {
            integerTableMap.remove(stringTableMap.get(name).getFile().getId());
        }
        Table table = new Table(file, name, pkeyField);
        stringTableMap.put(name,table);
        integerTableMap.put(file.getId(),table);
    }

    /**
     * 向目录中添加一个无主键的新表。
     * @param file      表文件
     * @param name      表名
     */
    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * 向目录中添加一个表明为 UUID 的无主键新表。
     * @param file  表文件
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * 返回具有指定名称的表的 id
     * @param name      指定表名称
     * @return
     * @throws NoSuchElementException
     */
    public int getTableId(String name) throws NoSuchElementException {
        if(stringTableMap.containsKey(name)){
            return stringTableMap.get(name).getFile().getId();
        }
        throw new NoSuchElementException();
    }

    /**
     * 返回指定表的元组描述(约束)
     * @param tableid       表的 id
     * @return
     * @throws NoSuchElementException
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        if(integerTableMap.containsKey(tableid)){
            return integerTableMap.get(tableid).getFile().getTupleDesc();
        }
        throw new NoSuchElementException();
    }

    /**
     * 返回指定表文件 DbFile
     * @param tableid   表的 id
     * @return
     * @throws NoSuchElementException
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        if(integerTableMap.containsKey(tableid)){
            return integerTableMap.get(tableid).getFile();
        }
        throw new NoSuchElementException();
    }

    /**
     * 获取主键
     * @param tableid   表的 id
     * @return
     */
    public String getPrimaryKey(int tableid) {
        if(integerTableMap.containsKey(tableid)){
            return integerTableMap.get(tableid).getPkeyField();
        }
        throw new NoSuchElementException();
    }

    /**
     * 获取表的迭代器
     * @return
     */
    public Iterator<Integer> tableIdIterator() {
        return integerTableMap.keySet().iterator();
    }

    /**
     * 获取表名
     * @param tableid   表的 id
     * @return
     */
    public String getTableName(int tableid) {
        if(integerTableMap.containsKey(tableid)){
            return integerTableMap.get(tableid).getName();
        }
        throw new NoSuchElementException();
    }

    /**
     * 从目录中删除所有表
     */
    public void clear() {
        integerTableMap.clear();
        stringTableMap.clear();
    }

    /**
     * 从文件中读取约束并在数据库中创建适当的表
     * @param catalogFile   目录文件
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            // 读取约束
            while ((line = br.readLine()) != null) {

                String name = line.substring(0, line.indexOf("(")).trim();
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int")) {
                        types.add(Type.INT_TYPE);
                    }
                    else if (els2[1].trim().toLowerCase().equals("string")) {
                        types.add(Type.STRING_TYPE);
                    }
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk")) {
                            primaryKey = els2[0].trim();
                        }
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

