package kvstore;

import java.util.LinkedList;


public class ThreadPool {

    /* Array of threads in the threadpool */
    public Thread threads[];
    private LinkedList<Runnable> jobQueue;

    /**
     * Constructs a Threadpool with a certain number of threads.
     *
     * @param size number of threads in the thread pool
     */
    public ThreadPool(int size) {
        threads = new Thread[size];
        jobQueue = new LinkedList<Runnable>();
        // implement me
        for(int i = 0; i < size; i++){
        		threads[i] = new WorkerThread(this);
        		threads[i].start();
        }
    }

    /**
     * Add a job to the queue of jobs that have to be executed. As soon as a
     * thread is available, the thread will retrieve a job from this queue if
     * if one exists and start processing it.
     *
     * @param r job that has to be executed
     * 
     */
    public synchronized void addJob(Runnable r){
        // implement me
    		jobQueue.addLast(r);
    		this.notify();
    }

    /**
     * Block until a job is present in the queue and retrieve the job
     * @return A runnable task that has to be executed
     * @throws InterruptedException if thread is interrupted while in blocked
     * state. Your implementation may or may not actually throw this.
     */
    public synchronized Runnable getJob() throws InterruptedException {
        // implement me
    		if(0 == jobQueue.size())
    			this.wait();
	
        return jobQueue.removeFirst();
    }

    /**
     * A thread in the thread pool.
     */
    public class WorkerThread extends Thread {

        public ThreadPool threadPool;

        /**
         * Constructs a thread for this particular ThreadPool.
         *
         * @param pool the ThreadPool containing this thread
         */
        public WorkerThread(ThreadPool pool) {
            threadPool = pool;
        }

        /**
         * Scan for and execute tasks.
         */
        @Override
        public void run() {
            // implement me
        		while(true){
        			try {
						threadPool.getJob().run();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
        		}
        }
    }
}
