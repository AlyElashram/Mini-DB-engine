import java.io.*;
import java.math.BigDecimal;
import java.util.*;

public class Table implements Serializable {
    String tableName;
    int numberOfPages;
    String keyType;
    int pageSize;
    int bucketSize;
    Vector<Grid> index=new Vector<Grid>();
    Hashtable<Integer,Object> minOfPage=new Hashtable<Integer,Object>();
    Hashtable<Integer,Object> maxOfPage=new Hashtable<Integer,Object>();
    Hashtable<Integer,Integer> numberOfRecords=new Hashtable<Integer,Integer>();

    public Table(String tableName) {
        this.tableName = tableName;
        this.numberOfPages = 0;
        this.bucketSize=bucketSize();
    }
    public void addGrid(Grid d){
        this.index.add(d);
    }
    public void addPage() {
        this.numberOfPages++;
    }
    public void setKeyType(String key,int size){
        this.keyType=key;
        this.pageSize=size;
    }
    public void setMin(int number,Record a){
        
        Object minimum=minOfPage.get(number);
       
        if(minimum==null){
            if(a.Datekey!=null){
                minOfPage.put(number,a.Datekey);

            }
            else{
                minOfPage.put(number,a.key);
            }

        }else{
           Object minCheck=a.key;
           if(minCheck==null){
               minCheck=a.Datekey;
           }
           if(keyType.equals("class java.lang.Integer")){
              int minCheck1=Integer.parseInt(minCheck.toString());
              int  minimum1=Integer.parseInt(minimum.toString());
               if(minCheck1<minimum1){
                   minOfPage.replace(number, minCheck1);
               }
               
            }
           else if(keyType.equals("class java.math.BigDecimal")){
            BigDecimal minCheck1=new BigDecimal(minCheck.toString());
            BigDecimal  minimum1=new BigDecimal(minimum.toString());
             if(minCheck1.compareTo(minimum1)<0){
                 minOfPage.replace(number, minCheck1);
             }
            }
            else if(keyType.equals("class java.lang.String")){
                String minCheck1=minCheck.toString();
                String minimum1=minimum.toString();
                if(minCheck1.compareTo(minimum1)<0){
                    minOfPage.replace(number, minCheck1);
                }

            }
            else{
                minCheck=a.Datekey;
                Date minCheck1=(Date)minCheck;
                Date minimum1=(Date)minimum;
                if(minCheck1.compareTo(minimum1)<0){
                    minOfPage.replace(number, minCheck1);
                }
            }
        }
    }
    public void setMax(int number,Record a){
        Object maximum=maxOfPage.get(number);
        
        if(maximum==null){
            if(a.Datekey!=null){
                maxOfPage.put(number,a.Datekey);

            }
            else{
                maxOfPage.put(number,a.key);
            }

        }else{
           Object maxCheck=a.key;
           if(maxCheck==null){
            maxCheck=a.Datekey;
        }
           if(keyType.equals("class java.lang.Integer")){
              int maxCheck1=Integer.parseInt(maxCheck.toString());
              int  maximum1=Integer.parseInt(maximum.toString());
               if(maxCheck1>maximum1){
                maxOfPage.replace(number, maxCheck1);
               }
               
            }
           else if(keyType.equals("class java.math.BigDecimal")){
            BigDecimal maxCheck1=new BigDecimal(maxCheck.toString());
            BigDecimal  maximum1=new BigDecimal(maximum.toString());
             if(maxCheck1.compareTo(maximum1)>0){
                maxOfPage.replace(number, maxCheck1);
             }
            }
            else if(keyType.equals("class java.lang.String")){
                String maxCheck1=maxCheck.toString();
                String maximum1=maximum.toString();
                if(maxCheck1.compareTo(maximum1)>0){
                    maxOfPage.replace(number, maxCheck1);
                }

            }
            else{
                
                maxCheck=a.Datekey;
                Date maxCheck1=(Date)maxCheck;
                try{
                Date maximum1=(Date)maximum;
                if(maxCheck1.compareTo(maximum1)>0){
                    maxOfPage.replace(number, maxCheck1);
                }
                }catch(Exception e){
                    maxOfPage.replace(number, maxCheck1);
                }
               
            }
        }
    }
    public void addRecord(int number){
        if(numberOfRecords.get(number)==null){
            numberOfRecords.put(number,1);
        }else{
            numberOfRecords.replace(number, numberOfRecords.get(number)+1);
        }
    }
    public void removeRecord(int number){
        int oldValue=numberOfRecords.get(number);
        numberOfRecords.replace(number, oldValue-1);
    }
    
    public void removeMin(int number){
        minOfPage.replace(number, null);
    }
    
    public void removeMax(int number){
        maxOfPage.replace(number,-1);
    }




    public static int bucketSize(){
        Properties prop = new Properties();
        String fileName = "src\\main\\resources\\DBApp.config";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        try {
            prop.load(is);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));

            }
}
