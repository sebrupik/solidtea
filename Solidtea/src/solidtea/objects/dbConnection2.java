package solidtea.objects;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import solidtea.Solidtea;
import solidtea.exceptions.NullDBConnectionException;

/**
 * Holds multiple JDBC connection objects, and allows manipulation of their configuration
 * and provides method for their use.
 * 
 * Created for the v6macassocgui application
 * 
 * 
 * @author snr
 */
public class dbConnection2 {
    private final String _class;
    private Solidtea owner;
    private Properties props;
    
    private HashMap<String, dbCon2Object> dbConHMap;
    
    public static final int CONNECTION_NO = -1;
    public static final int OTHER_ERROR = -2;
    
    public dbConnection2(Solidtea owner, Properties sysProps) {
        this._class = this.getClass().getName();
        this.owner = owner;
        this.props = sysProps;
        
        dbConHMap = new HashMap<>();
        
        createConnectionContainers();
    }
    
    /**
     * Reads the properties file and creates the Hashmap containers for the previously configured DB connections
     * specified in the properties file.
     * 
     */
    private void createConnectionContainers() {
        System.out.println(_class+"/createConnectionContainers entered...");
        String name;
        Iterator it = ((Set<String>)props.stringPropertyNames()).iterator();
        while(it.hasNext()) {
            name = (String)it.next();
            if(name.startsWith("db.")) {
                name = name.substring(name.indexOf(".")+1, name.lastIndexOf(".")); //the name of the DB connection object
                
                if(!dbConHMap.containsKey(name)) {
                    try {
                    System.out.println("Adding dbCon2Object "+ name);
                    dbConHMap.put(name, new dbCon2Object(new String[]{name,
                                                                      props.getProperty("db."+name+".server"),
                                                                      props.getProperty("db."+name+".username")},
                                                                    owner.loadPropsFromFile(props.getProperty("db."+name+".psfile"),false), null));
                    } catch (java.lang.NullPointerException npe) { owner.log(Level.SEVERE, _class, "createConnectionContainers", npe); }
                    System.out.println("Was the new dbconobject ("+name+") successfully added : "+dbConHMap.containsKey(name));
                }
            }
        }
    }
    
    public void createConnection(String dbName, String server, String usr, String pwd) { 
        dbCon2Object tmpDBCO = getDBCO(dbName);
        this.closeConnection(tmpDBCO);
        this.clearPreparedStatements(tmpDBCO);
        try {
            tmpDBCO.connection = DriverManager.getConnection("jdbc:mysql://"+server+"?user="+usr+"&password="+pwd);
            tmpDBCO.connection.setAutoCommit(false);
            
            this.createPreparedStatements(tmpDBCO);
        } catch (SQLException sqle) { owner.log(Level.SEVERE, _class, "createConnection", sqle);
        }
    }
    
    public void closeConnection(dbCon2Object dbco) {
        if(dbco.connection != null) {
            try {
                System.out.println(_class+"/closeConnection - attempting");
                dbco.connection.close();
                System.out.println(_class+"/closeConnection - done");
            } catch(SQLException sqle) { owner.log(Level.SEVERE, _class, "closeConnection", sqle); }
        }
    }
        
    public void clearPreparedStatements(dbCon2Object dbco) {
        System.out.print(_class+"/clearPreparedStatements....");
        if(dbco.psHash!=null)
            dbco.psHash.clear();
        System.out.println("DONE");
    }
    
    public void createPreparedStatements(dbCon2Object dbco) throws SQLException {
        if(dbco.psHash == null) 
            dbco.psHash = new HashMap<>();
        //if(dbco.psHash != null && !dbco.psHash.isEmpty()) {  // no point building it again!
        if(!dbco.psHash.isEmpty()) 
            dbco.psHash.clear();
            
        //dbco.psHash = new HashMap<>();
        String tS, tP;

        for (Iterator i = dbco.psProps.stringPropertyNames().iterator(); i.hasNext();) {
            tS = (String)i.next();
            tP = dbco.psProps.getProperty(tS);

            if(tP ==null) { 
                owner.log(Level.SEVERE, _class, "createPreparedStatement", "No value returned for "+tS+". This will not end well. Exiting!");
                System.exit(0);
            }
            //dbco.psHash.put(tS, this.createPreparedStatement(dbco, dbco.psProps.getProperty(tS)));
            if(dbco.connection!=null) {
                dbco.psHash.put(tS, dbco.connection.prepareStatement(tP));
            }
        }
        owner.log(Level.INFO, _class, "createPreparedStatement", "psHash size for "+dbco.attributes[0]+" is: "+dbco.psHash.size());
    }
    
    public int executeUpdate(String dbName, PreparedStatement ps) {  return this.executeUpdate(dbName, new PreparedStatement[]{ps}); }
    public int executeUpdate(String dbName, PreparedStatement[] ps) {
        dbCon2Object tmpDBCO = getDBCO(dbName);
        Statement stmt = null;
        int updates = 0;
        if (tmpDBCO.connection != null) {
            try {
                stmt = tmpDBCO.connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                for (PreparedStatement p : ps) {
                    System.out.println(p);
                    updates += p.executeUpdate();
                }
                tmpDBCO.connection.commit();
                //savepointTable.addRow(new Object[]{dbcon.setSavepoint(), ps[0].toString()});
                //System.out.println("dbCon columncount is "+savepointTable.getColumnCount());
                return updates;
            } catch (SQLException sqle) {
                try { tmpDBCO.connection.rollback(); } catch (SQLException sqle2) { owner.log(Level.SEVERE, _class, "executeUpdate(PS[])(rollback)", sqle2); }
                owner.log(Level.SEVERE, _class, "executeUpdate(PS[])", sqle);
            } finally {
                if (stmt != null) {
                    try { stmt.close(); } catch (SQLException sqle) { owner.log(Level.SEVERE, _class, "executeUpdate(PS[])(finally)", sqle); }
                }
            }

        } else { return this.CONNECTION_NO; }
        return this.OTHER_ERROR;
    }
    
    public ResultSet executeQuery(String dbName, PreparedStatement ps) {
        dbCon2Object tmpDBCO = getDBCO(dbName);
        System.out.println(ps);
        if (tmpDBCO.connection != null) {
            try {
                //stmt = dbcon.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                return ps.executeQuery();
            } catch (SQLException sqle) { owner.log(Level.SEVERE, _class, "executeQuery(ps)", sqle);
            }
        } 
        return null;
    }
    
    public PreparedStatement getPS(String dbName, String psname) {
        return getDBCO(dbName).psHash.get(psname);
    }
    
    public dbCon2Object getDBCO(String name) {
        return dbConHMap.get(name);
    }
    
    public String[] getKeys() {
        return dbConHMap.keySet().toArray(new String[0]);
    }
    
    public void closeAllConnections() {
        String[] keys = this.getKeys();
        
        for (String key : keys) 
            this.closeConnection(dbConHMap.get(key));
    }
}