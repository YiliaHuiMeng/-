package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class QuestionTwo {
private static Configuration conf = null;
	
	static{
		conf = HBaseConfiguration.create();
	}
	
	public static void creatTable(String tableName,String[] fields) throws Exception{
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		if(admin.tableExists(tableName)){
			admin.disableTable(tableName);         
	        admin.deleteTable(tableName);
			System.out.println("table already exists and delete it!");
		}
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		for(int i = 0; i < fields.length;i++){
			tableDesc.addFamily(new HColumnDescriptor(fields[i]));
		}
		admin.createTable(tableDesc);
		System.out.println(tableName+"ok!");
	}
	
	
	 /**     
     * 删除一行记录     
     */        
    public static void delRecord (String tableName, String rowKey) throws IOException{         
        HTable table = new HTable(conf, tableName);         
        List list = new ArrayList();         
        Delete del = new Delete(rowKey.getBytes());         
        list.add(del);         
        table.delete(list);         
        System.out.println("del recored " + rowKey + " ok.");         
    } 
    /**     
     * 插入记录     
     */        
    public static void addRecord (String tableName, String row, String[] fields, String[] values)         
            throws Exception{         
        try {         
            HTable table = new HTable(conf, tableName);         
            for(int i = 0;i != fields.length;i++){
            	Put put = new Put(row.getBytes());
            	String[] cols = fields[i].split(";");
            	put.addColumn(cols[0].getBytes(), cols[1].getBytes(), values[i].getBytes());
            	table.put(put);
            }        
        } catch (IOException e) {         
            e.printStackTrace();         
        }         
    } 
    

    public static void scanColumn (String tableName,String column)         
            throws Exception{         
        try {         
            HTable table = new HTable(conf, tableName);         
            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes(column));
            ResultScanner scanner = table.getScanner(scan);
            for(Result result = scanner.next();result != null;result = scanner.next()){
            	showCell(result);
            }
        } catch (IOException e) {         
            e.printStackTrace();         
        }         
    } 
    
    
    private static void showCell(Result result) {
		// TODO Auto-generated method stub
		Cell[] cells = result.rawCells();
		for(Cell cell:cells){
			System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
			System.out.println("Timetamp:"+cell.getTimestamp()+" ");
			System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
			System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
			System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
		}
	}
    
    public static void modifyData(String tableName,String row,String column,String value) throws IOException{
    	try{
    		HTable table = new HTable(conf, tableName);
    		Put put = new Put(row.getBytes());
    		put.addColumn(column.getBytes(), null, value.getBytes());
    		table.put(put);
    	} catch (IOException e){         
            e.printStackTrace();         
        }  
    }

	public static void countRows(String tableName) throws IOException{
    	try{
    		HTable table = new HTable(conf, tableName);
    		Scan scan = new Scan();
    		ResultScanner scanner = table.getScanner(scan);
    		int num = 0;
    		for(Result result = scanner.next(); result != null;result = scanner.next()){
    			num++;
    		}
    		System.out.println("number of rows:"+num);
    		
    	} catch (IOException e){         
            e.printStackTrace();         
        }  
    }
 
    public static void  main (String [] agrs) {         
        try {         
            String tablename = "scores";         
            String[] familys = {"grade", "course"};         
            QuestionOne.creatTable(tablename, familys);         
                      
            //add record zkb         
            QuestionOne.addRecord(tablename,"zkb","grade","","5");         
            QuestionOne.addRecord(tablename,"zkb","course","","90");         
            QuestionOne.addRecord(tablename,"zkb","course","math","97");         
            QuestionOne.addRecord(tablename,"zkb","course","art","87");         
            //add record  baoniu         
            QuestionOne.addRecord(tablename,"baoniu","grade","","4");         
            QuestionOne.addRecord(tablename,"baoniu","course","math","89"); 
            QuestionOne.addRecord(tablename,"baoniu","course","art","85"); 
                      
            System.out.println("===========get one record========");         
            QuestionOne.getOneRecord(tablename, "zkb");         
                      
            System.out.println("===========show all record========");         
            QuestionOne.getAllRecord(tablename);         
                      
            System.out.println("===========del one record========");         
            //Hello.delRecord(tablename, "baoniu");         
           // Hello.getAllRecord(tablename);         
                      
            System.out.println("===========show all record========");         
            QuestionOne.getAllRecord(tablename); 
            
           // Hello.deleteTable(tablename);
            //Hello.creatTable(tablename, familys);
            
        } catch (Exception e) {         
            e.printStackTrace();         
        }         
    }  

}
