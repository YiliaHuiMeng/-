package hiveHello;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
	
	public class QuestionOne {
	    private static String driverName = 
	                   "org.apache.hive.jdbc.HiveDriver";
	  
	    public static void main(String[] args) 
	                            throws SQLException {
	    	
	    	
	    	
	    	try {
	            Class.forName(driverName);
	        } catch (ClassNotFoundException e) {
	            e.printStackTrace();
	            System.exit(1);
	        }

	        Connection con = DriverManager.getConnection(
	                           "jdbc:hive2://master:10000/default", "hive", "Xuzhenhua123456/");
	        Statement stmt = con.createStatement();
	        String tableName = "usr";
	        String sql="";
	        String filepath="/home/xzh/usr.txt";
	        stmt.execute("drop table if exists " + tableName);
	        stmt.execute("create table "
					+ tableName
					+ " (id int, name string,age int,hobby string) row format delimited fields terminated by '，' lines terminated by '\n' stored as textfile");
	        System.out.println("Create table success!");
	          //导入数据
	        sql = "LOAD DATA LOCAL INPATH '" + filepath
					+ "'OVERWRITE INTO TABLE " + tableName;
	        
	         stmt.execute(sql);
	         
	         
	         
	        // show tables
	        sql = "show tables '" + tableName + "'";
	        System.out.println("Running: " + sql);
	        ResultSet res = stmt.executeQuery(sql);
	        if (res.next()) {
	            System.out.println(res.getString(1));
	        }

	        // describe table
	        sql = "describe " + tableName;
	        System.out.println("Running: " + sql);
	        res = stmt.executeQuery(sql);
	        while (res.next()) {
	            System.out.println(res.getString(1) + "\t" + res.getString(2));
	        }

	        System.out.println("=========打印表数据=========");
	        sql = "select * from " + tableName;
	        res = stmt.executeQuery(sql);
	        while (res.next()) {
	            System.out.println(res.getInt(1) + "\t"
	                                               + res.getString(2)+"\t"+res.getInt(3)+"\t"+res.getString(4));
	        }
	        
	        sql="select count(id) from usr where hobby='football'";
	        res=stmt.executeQuery(sql);
	        while(res.next()){
	        	System.out.println("喜欢football的学生个数为："+res.getInt(1)+"==========");
	        }
	        con.close();
	    }
	}


