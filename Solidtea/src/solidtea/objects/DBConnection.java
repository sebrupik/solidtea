package solidtea.objects;

import solidtea.exceptions.NullDBConnectionException;

import java.io.*;
import java.util.HashMap;
import java.util.Properties;
import java.sql.*;

public class DBConnection {
    private final String _class;
    private String _server="127.0.0.1";
    private String _username="root";
    private String _password="password";

    //private String server, username, password;
    private Connection dbcon;
    private HashMap<String, PreparedStatement> psHash;
    Properties psProps;

    public DBConnection(String server, String username, String password, Properties psProps)  {
        this._server = server;
        this._username = username;
        this._password = password;
        this.psProps = psProps;

        this._class = this.getClass().getName();

        this.createConnection(server, username, password);
    }

    public void closeConnection() {
        if(dbcon != null) {
            try {
                System.out.println(_class+"/closeDBConnection - attempting");
                dbcon.close();
                System.out.println(_class+"/closeDBConnection - done");
            } catch(SQLException sqle) { System.out.println(_class+"/"+sqle); }
        }
    }

    public boolean recreateConnection() {
        this.createConnection(_server, _username, _password);

        return true;
    }


    public void createConnection(String server, String usr, String pwd) { 
        closeConnection();
        clearPreparedStatements();
        try {
            dbcon = DriverManager.getConnection("jdbc:mysql://"+server+"?user="+usr+"&password="+pwd);
            dbcon.setAutoCommit(false);
            //this.saveProperties("lastSQLServerIP", server);
            //this.saveProperties("lastSQLuser", usr);
        } catch (SQLException sqle) { 
            System.out.println(_class+"/createConection - ");
            System.out.println("SQLException: " + sqle.getMessage()); 
            System.out.println("SQLState: " + sqle.getSQLState()); 
            System.out.println("VendorError: " + sqle.getErrorCode()); 
        }
    }

    private void clearPreparedStatements() {
        System.out.print(_class+"/clearPreparedStatements....");
        if(psHash!=null)
            psHash.clear();
        System.out.println("DONE");
    }

    public PreparedStatement getPS(String psname) {
        System.out.println(_class+"/getPS - returning PS : "+psname);
        PreparedStatement p = null;
        if(psHash == null)
            psHash = new HashMap<>();

        String item = psProps.getProperty(psname);
        if(item==null) {
            System.err.println("PreparedStatement "+psname+" missing!! -:FATAL:- Exiting.");
            System.exit(0);
        } else {
            psHash.put(psname, this.createPreparedStatement(item));
            p = (PreparedStatement)psHash.get(psname);
        }
        return p;
    }

    public PreparedStatement createPreparedStatement(String s) throws NullDBConnectionException {
        System.out.println(_class+"/createPreparedStatement - attepting to create PS : "+s);
        if(dbcon !=null) {
            try {
                return dbcon.prepareStatement(s);
            } catch (SQLException sqle) {
                System.out.println(_class+"/getPreparedStatment - ");
                System.out.println("SQLException: " + sqle.getMessage());
                System.out.println("SQLState: " + sqle.getSQLState());
                System.out.println("VendorError: " + sqle.getErrorCode());
            }
        }
        throw new NullDBConnectionException(_class+"/createPreparedStatement - dbcon is null!");
    }

    public int executeUpdate(PreparedStatement ps) throws NullDBConnectionException {  return this.executeUpdate(new PreparedStatement[]{ps}); }
    public int executeUpdate(PreparedStatement[] ps) throws NullDBConnectionException {
        Statement stmt = null;
        int updates = 0;
        if (dbcon != null) {
            try {
                stmt = dbcon.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                // for(int i=0; i<ps.length; i++) {
                //    System.out.println(ps[i]);
                //    updates += ps[i].executeUpdate();
                //}
                for(PreparedStatement element : ps) {
                    //System.out.println(element);
                    updates += element.executeUpdate();
                }
                dbcon.commit();
                //savepointTable.addRow(new Object[]{dbcon.setSavepoint(), ps[0].toString()});
                //System.out.println("dbCon columncount is "+savepointTable.getColumnCount());
                return updates;
            } catch (SQLException sqle) {
                try { dbcon.rollback(); } catch (SQLException sqle2) { System.out.println(_class+"/executeUpdate(PS)(rollback) - "+sqle2); }
                System.out.println(_class+"/executeUpdate(PS) - ");
                System.out.println("SQLException: " + sqle.getMessage());
                System.out.println("SQLState: " + sqle.getSQLState());
                System.out.println("VendorError: " + sqle.getErrorCode());
            } finally {
                if (stmt != null) {
                    try { stmt.close(); } catch (SQLException sqle) { System.out.println(_class+"/executeUpdate(PS)(finally) - "+sqle); }
                }
                stmt = null;
            }

        } else { throw new NullDBConnectionException(); }
        throw new NullDBConnectionException();
    }

    public ResultSet executeQuery(String q) throws NullDBConnectionException {
        System.out.println(q);
        Statement stmt = null;
        if (dbcon != null) {
            try {
                stmt = dbcon.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                return stmt.executeQuery(q);
            } catch (SQLException sqle) {
                System.out.println(_class+"/executeQuery(S) - ");
                System.out.println("SQLException: " + sqle.getMessage());
                System.out.println("SQLState: " + sqle.getSQLState());
                System.out.println("VendorError: " + sqle.getErrorCode());
            }
        } else { throw new NullDBConnectionException(); }
        throw new NullDBConnectionException();
    }

    public ResultSet executeQuery(PreparedStatement ps) throws NullDBConnectionException {
        System.out.println(ps);
        //Statement stmt = null;
        if (dbcon != null) {
            try {
                //stmt = dbcon.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                return ps.executeQuery();
            } catch (SQLException sqle) {
                System.out.println(_class+"/executeQuery(PS) - ");
                System.out.println("SQLException: " + sqle.getMessage());
                System.out.println("SQLState: " + sqle.getSQLState());
                System.out.println("VendorError: " + sqle.getErrorCode());
            }
        } else { throw new NullDBConnectionException(); }
        throw new NullDBConnectionException();
    }
    
    public boolean execute(String s) throws NullDBConnectionException {
        System.out.println(s);
        Statement stmt = null;
        if (dbcon != null) {
            try {
                stmt = dbcon.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                return stmt.execute(s);
            } catch (SQLException sqle) { 
                System.out.println(_class+"/execute(s) - ");
                System.out.println("SQLException: " + sqle.getMessage()); 
                System.out.println("SQLState: " + sqle.getSQLState()); 
                System.out.println("VendorError: " + sqle.getErrorCode()); 
            } 
        } else { throw new NullDBConnectionException(); }
        //throw new NullDBConnectionException();
        return false;  //added 2006/12/05  possibly revert back to above line
    }

    public Statement createStatement() throws SQLException {
        return dbcon.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    }
    /*private void saveProperties(String key, String value) {
        try {
            java.security.AccessController.checkPermission(new FilePermission("client.properties","write"));
            try {
                props.setProperty(key, value);
                props.store( new FileOutputStream(new File("client.properties")), "");
            } catch (IOException ioe) { System.out.println(ioe); }

        } catch(java.lang.SecurityException se) { System.out.println(_class+"/saveProperties - "+se); }
    }*/

    /* public String getProperty(String prop) {
        if (!props.isEmpty()) {
            System.out.println("returning prop : "+props.getProperty(prop));
            return props.getProperty(prop);
        }
        throw new NullPointerException("There appears to be no properties file.");
    } */

    public boolean isConnected() {
        if(dbcon != null) {
            try { return !dbcon.isClosed();
            } catch(SQLException sqle) { System.out.println(sqle); return false; }
        } else { return false; }
    }

    public void getClientInfo() {
        System.out.println(_class+"/getClientInfo - ");
        try {
            Properties prop = dbcon.getClientInfo();
            for (java.util.Enumeration e = prop.propertyNames(); e.hasMoreElements();)
                System.out.println(e.nextElement());
        } catch(SQLException sqle) { System.out.println(_class+"/getClientInfo - "+sqle); }
    }

    public DatabaseMetaData getMetaData() throws SQLException { return this.dbcon.getMetaData(); }
    public DatabaseMetaData getDBMD() throws SQLException { return dbcon.getMetaData(); }
    
    public String getConnectionUserNameShort() throws SQLException {
        String s = this.getMetaData().getUserName();
        return s.substring(0, s.indexOf("@"));
    }
}