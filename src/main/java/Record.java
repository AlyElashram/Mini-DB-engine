import java.io.*;
import java.util.*;
public class Record implements Serializable {
         Vector<Tuple> tuples;
         String tableName;
         String clusteringKey;
         String keyType;
         String key;
         Date Datekey;
         int recordSize;
        
        Record(String TableName,Vector<Tuple> colNameValue){
            this.tableName=TableName;
           this.recordSize=colNameValue.elementAt(0).csvLines;
           tuples=new Vector<>(recordSize);
        
           colNameValue.forEach((element)->{
                if(element.isClustering()){
                    clusteringKey=element.getColName().toString();
                    this.keyType=element.getValue().getClass().toString();
                    if(keyType.equals("class java.util.Date")){
                        this.Datekey=(Date)element.getValue();
                    }
                    else{
                    this.key=element.getValue().toString();
                    }

                }
                tuples.add(element);
                
            }); 
        
    }
    public int contains(Tuple t){

        for(int i=0;i<this.tuples.size();i++){
            Tuple current=this.tuples.get(i);
            if(current.getColName().toString().equals(t.getColName().toString())){
                return i;
            }
        }
        return -1;


    }
    
       
        
    
    
      
    }
     

