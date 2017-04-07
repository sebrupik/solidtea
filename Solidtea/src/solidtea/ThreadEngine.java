package solidtea;

import solidtea.interfaces.RejectedExecutionHandlerImpl;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

abstract class ThreadEngine {
    RejectedExecutionHandlerImpl rejectionHandler;
    ThreadFactory threadFactory;
    ThreadPoolExecutor executorPool;
    
    public ThreadEngine(int corePoolSize, int maximumPoolSize, long keepAliveTime) {
        rejectionHandler = new RejectedExecutionHandlerImpl();
        threadFactory = Executors.defaultThreadFactory();
        executorPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2), threadFactory, rejectionHandler);
    }
    
    abstract void execute() ;
    
    public void shutdown() {
        executorPool.shutdown();
    }    
}
