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
            	String[] cols = fields[i].split(":");
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
            String tablename = "Student";         
            String[] familys = {"S_No", "S_Name","S_Sex","S_Age"}; 
            String[] values1 = {"2015002","Zhangsan","male","23"};
            String[] values2 = {"2015002","Mary","famale","22"};
            String[] values3 = {"2015003","Lisi","male","24"};
            QuestionTwo.creatTable(tablename, familys);         
                      
            //add record Student         
            QuestionTwo.addRecord(tablename,"s001",familys,values1);         
            QuestionTwo.addRecord(tablename,"s002",familys,values2);           
            QuestionTwo.addRecord(tablename,"s003",familys,values3);                    
            
            String tablename1 = "Course";         
            String[] familys1 = {"C_No", "C_Name","C_Credit"}; 
            String[] values4 = {"123001","Math","2.0"};
            String[] values5 = {"123002","Computer Science","5.0"};
            String[] values6 = {"123003","English","3.0"};
            QuestionOne.creatTable(tablename1, familys1); 
            //add record  Course         
            QuestionTwo.addRecord(tablename,"c001",familys1,values4);         
            QuestionTwo.addRecord(tablename,"c002",familys1,values5);
            QuestionTwo.addRecord(tablename,"c003",familys1,values6); 
            
            String tablename2 = "SC";         
            String[] familys2 = {"SC_No", "SC_Cno","SC_Score"}; 
            String[] values7 = {"2015001","123001","86"};
            String[] values8 = {"2015001","123003","69"};
            String[] values9 = {"2015002","123002","77"};
            String[] values10 = {"2015002","123003","99"};
            String[] values11 = {"2015003","123001","98"};
            String[] values12 = {"2015003","123002","95"};
            QuestionOne.creatTable(tablename2, familys2); 
            //add record  SC         
            QuestionTwo.addRecord(tablename,"sc001",familys2,values7);         
            QuestionTwo.addRecord(tablename,"sc002",familys2,values8);
            QuestionTwo.addRecord(tablename,"sc003",familys2,values9);
            QuestionTwo.addRecord(tablename,"sc001",familys2,values10);         
            QuestionTwo.addRecord(tablename,"sc002",familys2,values11);
            QuestionTwo.addRecord(tablename,"sc003",familys2,values12); 
                      
            System.out.println("===========get one record========");         
            QuestionOne.getOneRecord(tablename, "2015001");         
                      
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
