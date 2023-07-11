import cluster.management.LeaderElection;
import cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final int DEFAULT_PORT = 8080;
    private ZooKeeper zookeeper;


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        Application app = new Application();
        ZooKeeper zookeeper = app.connectToZooKeeper();

        ServiceRegistry serviceRegistry = new ServiceRegistry(zookeeper);

        OnElectionAction onElectionAction = new OnElectionAction(serviceRegistry, currentServerPort);

        LeaderElection leaderElection = new LeaderElection(zookeeper, onElectionAction);
        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();

        //timer start
        app.run();
        //timer end

        System.out.println(timer end - timer start);
        app.close();
        System.out.println("Disconnecting Zookeeper, exiting application");
    }

    public ZooKeeper connectToZooKeeper() throws IOException {
        this.zookeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zookeeper;
    }

    public void run() throws InterruptedException{
        synchronized (zookeeper){
            zookeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zookeeper.close();
    }

    @Override
    public void process(WatchedEvent event)  {
        switch (event.getType()) {

            case None:
                if(event.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("Successfully Connected to Zookeeper!");
                }else{
                    synchronized (zookeeper){
                        System.out.println("Disconnected to Zookeeper!");
                        zookeeper.notifyAll();
                    }
                }
        }

    }
}