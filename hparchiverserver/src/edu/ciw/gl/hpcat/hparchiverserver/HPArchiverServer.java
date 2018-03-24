package edu.ciw.gl.hpcat.hparchiverserver;


import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.ciw.hpcat.epics.data.CountDownConnection;
import edu.ciw.hpcat.epics.data.EpicsDataObject;


import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import javax.sql.DataSource;
public class HPArchiverServer implements PropertyChangeListener {
	List<String> pvlines = new ArrayList<String>();
	   CountDownConnection countDownConnection = CountDownConnection.getInstance();

	   // JDBC driver name and database URL
	   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String JDBC_DB_URL = "jdbc:mysql://localhost/hparchive";

	   //  Database credentials
	   static final String JDBC_USER = "username";
	   static final String JDBC_PASS = "password";
//	   Connection dbConnection = null;
	
	   Connection connObj = null;
	   
	   private static GenericObjectPool gPool = null;
	   
	   DataSource dataSource;
	   
	   public HPArchiverServer() {
		   
	   }
	   
	   
	    public DataSource setUpPool() throws Exception {

	        Class.forName(JDBC_DRIVER);	 

	        // Creates an Instance of GenericObjectPool That Holds Our Pool of Connections Object!

	        gPool = new GenericObjectPool();

	        gPool.setMaxActive(5);	 

	        // Creates a ConnectionFactory Object Which Will Be Use by the Pool to Create the Connection Object!

	        ConnectionFactory cf = new DriverManagerConnectionFactory(JDBC_DB_URL, JDBC_USER, JDBC_PASS);
	 

	        // Creates a PoolableConnectionFactory That Will Wraps the Connection Object Created by the ConnectionFactory to Add Object Pooling Functionality!

	        PoolableConnectionFactory pcf = new PoolableConnectionFactory(cf, gPool, null, null, false, true);

	        return new PoolingDataSource(gPool);

	    }

	 

	    public GenericObjectPool getConnectionPool() {

	        return gPool;
	    }
	    

	    
	    public void setJDBCConnection() {
			try {	
				dataSource = setUpPool();
			}catch(Exception sqlException) {
				sqlException.printStackTrace();
			} 
		}
		
	
		private static java.sql.Timestamp getCurrentTimeStamp() {

			java.util.Date today = new java.util.Date();
			return new java.sql.Timestamp(today.getTime());
		}
		  protected File getDir(String str) {

			    String dot = ".";
			    File thisDir = new File(dot, str);
			    return thisDir;
			  }
		  protected synchronized void addPv(String str) {

			    EpicsDataObject edo = new EpicsDataObject(str, true);
			    edo.addPropertyChangeListener("val", this);
			  }

		  public void writePvsToFile(String dirName) {
			  String pvReadFileName = dirName+"\\pvList.txt";			   
			  Path file = Paths.get(pvReadFileName);
			  try {
				Files.write(file, pvlines, Charset.forName("UTF-8"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }
		  
		  protected void readPvFile(String dirName, String str) {

			  String dateTime;
			    File pvFile = new File(dirName, str);
			    int pvCount = 0;

			    try {
				      BufferedReader in = new BufferedReader(new FileReader(pvFile));
			      String pvName;
			      while ( (pvName = in.readLine()) != null) {
			    	  dateTime = pvName.trim()+"   "+getCurrentTimeStamp().toString();
			        pvlines.add(dateTime);
			        addPv(pvName);
			        pvCount = pvCount+1;
			        if ((pvCount%5)==0)countDownConnection.pendIO();
			      }
			      countDownConnection.pendIO();
			      in.close();
			    }
			    catch (IOException e) {
			    }
			  }

	@Override
	public void propertyChange(PropertyChangeEvent evt) {
		PreparedStatement preparedStatement = null;		
		String dateTime;

		EpicsDataObject obj = (EpicsDataObject)evt.getNewValue();
		
		String pvName = obj.getPvName();
		String pvValue = obj.getVal();
		long timeNow = System.currentTimeMillis();
		dateTime = getCurrentTimeStamp().toString();

		String tableName = pvName.replace(':', '_');
		tableName = tableName.replace('.','_');
		
		String insertTableSQL = "INSERT INTO "+tableName+ " (pvname, pvvalue, dateTime,pvsavetime) VALUES"
				+ "(?,?,?,?)";

		try {
			connObj = dataSource.getConnection();
			preparedStatement = connObj.prepareStatement(insertTableSQL);
			
			preparedStatement.setString(1, pvName);
			preparedStatement.setString(2, pvValue);
			preparedStatement.setString(3, dateTime);			
			preparedStatement.setLong(4, timeNow);
			preparedStatement.executeUpdate();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				// Closing PreparedStatement Object
				if(preparedStatement != null) {
					preparedStatement.close();
				}
				// Closing Connection Object
				if(connObj != null) {
					connObj.close();
				}
			} catch(Exception sqlException) {
				sqlException.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		String pvDir = "C:\\PVListFiles";
		String fileName;
		HPArchiverServer hpserver = new HPArchiverServer();
		hpserver.setJDBCConnection();
	
		
		File folder = new File(pvDir);
		File[] listOfFiles = folder.listFiles();

	    for (int i = 0; i < listOfFiles.length; i++) {
	       fileName = listOfFiles[i].getName();
	       hpserver.readPvFile(pvDir, fileName);	
	      }

	    hpserver.writePvsToFile("c:\\archiverPVList");

	}

}
