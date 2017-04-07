package solidtea.objects;

import v6macassoc.V6MACassoc;

public class ShutdownThread extends Thread {
    V6MACassoc v6MA;
    public ShutdownThread(V6MACassoc v6MA) {
        this.v6MA = v6MA;
    }
    
    @Override public void run() {
        System.out.println("Shutdown hook ran!");
        if(v6MA!=null)
            v6MA.shutdownThreads();
    }
    
}
