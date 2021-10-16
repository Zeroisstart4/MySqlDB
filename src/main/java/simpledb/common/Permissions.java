package simpledb.common;


/**
 * @author zhou <br/>
 *
 * 表示对关系/文件的请求权限的类   <br/>
 * 具有两个级别的权限            <br/>
 * 1. READ_ONLY                  <br/>
 * 2. READ_WRITE                 <br/>
 */
public class Permissions {
  int permLevel;

  private Permissions(int permLevel) {
    this.permLevel = permLevel;
  }

  @Override
  public String toString() {
    if (permLevel == 0) {
      return "READ_ONLY";
    }
    if (permLevel == 1) {
      return "READ_WRITE";
    }
    return "UNKNOWN";
  }

  public static final Permissions READ_ONLY = new Permissions(0);
  public static final Permissions READ_WRITE = new Permissions(1);

}
