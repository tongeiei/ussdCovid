package main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Scanner;

import javax.sql.DataSource;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

public class SubmitOMX {

	// JDBC Driver Name 
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   
 
    private static GenericObjectPool gPool = null;
 
    @SuppressWarnings("unused")
    public DataSource setUpPool(String JDBC_DB_URL,String JDBC_USER,String JDBC_PASS) throws Exception {
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
 
    // This Method Is Used To Print The Connection Pool Status
    private void printDbStatus() {
        System.out.println("Max.: " + getConnectionPool().getMaxActive() + "; Active: " + getConnectionPool().getNumActive() + "; Idle: " + getConnectionPool().getNumIdle());
    }
    public static void main(String[] args) throws Exception {
	
	     	Scanner scanner = new Scanner(System.in);
	     	
	     	System.out.println("Enter limit record.");
	     	String num_of_record = scanner.next();
	     	
	     	System.out.println("Enter delay time.");  	
	     	String delay_time = scanner.next();
	     	
	     	
	     	String host = "jdbc:mysql://172.19.198.164/MBX_COVID_DB";
	     	String user = "mvp_rw";
	     	String password = "P@ssw0rd@kurata";
	     	
	     	
	        ResultSet rsObj = null;
	        Connection connObj = null;
	        PreparedStatement pstmtObj = null;
	        SubmitOMX jdbcObj = new SubmitOMX();
	        try {   
	            DataSource dataSource = jdbcObj.setUpPool(host,user,password);
	            //jdbcObj.printDbStatus();
	 
	            // Performing Database Operation!
	            System.out.println("\n=====Making A New Connection Object For Db Transaction=====\n");
	            connObj = dataSource.getConnection();
	           // jdbcObj.printDbStatus(); 
	           
	            StringBuilder queryBuilder = new StringBuilder();
	            queryBuilder.append(" SELECT mvt.trans_id,mvt.msisdn,mvt.idcard_no,mvt.subtype,st.id FROM markuse_voice_trans mvt ");
	        	queryBuilder.append(" LEFT JOIN submit_trans st on mvt.idcard_no = st.idcard_no ");
	        	queryBuilder.append(" WHERE mvt.bypass_omx = 1 and st.id is null   ORDER BY mvt.create_date asc limit " + num_of_record);
	        	
	        	System.out.println(queryBuilder.toString());
	            pstmtObj = connObj.prepareStatement(queryBuilder.toString());
	            rsObj = pstmtObj.executeQuery();
	           
	            while (rsObj.next()) {
	                System.out.println("msisdn: " + rsObj.getString("msisdn"));
	            }
	            
	            System.out.println("\n=====Releasing Connection Object To Pool=====\n");            
	        } catch(Exception sqlException) {
	            sqlException.printStackTrace();
	        } finally {
	            try {
	                // Closing ResultSet Object
	                if(rsObj != null) {
	                    rsObj.close();
	                }
	                // Closing PreparedStatement Object
	                if(pstmtObj != null) {
	                    pstmtObj.close();
	                }
	                // Closing Connection Object
	                if(connObj != null) {
	                    connObj.close();
	                }
	            } catch(Exception sqlException) {
	                sqlException.printStackTrace();
	            }
	        }
	        //jdbcObj.printDbStatus();
			
	}
	
	
	
}
