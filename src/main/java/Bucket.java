import java.io.Serializable;
import java.util.Vector;

public class Bucket implements Serializable{
    String tableName;
    int  bucketSize;
    Vector<References> allReferences;

    public Bucket(String tableName,int bucketSize){
       this.tableName=tableName;
       this.bucketSize=bucketSize;
       this.allReferences=new Vector<References>(bucketSize);
    }
    public boolean addRef(References a){
        if(this.allReferences.size()==bucketSize){
            return false;
        }
        this.allReferences.add(a);
        return true;
    }
}
