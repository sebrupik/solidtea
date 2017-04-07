package solidtea;

import solidtea.objects.DBConnection;
import solidtea.objects.DBConThread;

import java.util.ArrayList;

public class DatabaseEngine extends ThreadEngine {
    private final String _CLASS;
    Solidtea owner;
    DBConnection dbcon;
    DBConThread dbct;
    
    public DatabaseEngine(Solidtea owner, DBConnection dbcon) {
        super(1,1,10);
        this.owner = owner;
        this.dbcon = dbcon;
        this._CLASS = this.getClass().getName();
        
        this.dbct = new DBConThread(dbcon); 
    }
    
    @Override public void execute() {
        executorPool.execute(dbct);
    }
    
    /**
     * 
     * @param type the content type of the Arraylist parameter, eg ipv6neigh
     * @param al Arraylist containing the objects to be parsed and inserted into the database
     */
    public void insertArrayList(String type, ArrayList al) {
        if(dbct!=null) 
            dbct.insertArrayList(type, al);
    }
}

