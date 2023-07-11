package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;


public class LeaderElection implements Watcher {
    private static final String ELECTION_NAMESPACE = "/election";
    private static final String REGISTRY_ZNODE = "/service_registry";
    private String currentZnodeName;
    private final ZooKeeper zookeeper;
    private final OnElectionCallback onElectionCallback;

    public LeaderElection(ZooKeeper zookeeper, OnElectionCallback onElectionCallback){
        this.zookeeper = zookeeper;
        this.onElectionCallback = onElectionCallback;
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {

        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zookeeper.create(znodePrefix,
                new byte[]{},
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reelectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        String predecessorZnodeName = "";

        while(predecessorStat == null){

            List<String> children = zookeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);

            String smallesChild = children.get(0);

            if(smallesChild.equals(currentZnodeName)){
                System.out.println("I am the leader!");
                onElectionCallback.onElectedToBeLeader();
                return;
            }else{
                System.out.println("I am not the leader");
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zookeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }

        }

        onElectionCallback.onWorker();

        System.out.println("Watching znode " + predecessorZnodeName);
        System.out.println();
//        System.out.println("I am not the leader, " + smallesChild + " is the leader");
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()){
            case NodeDeleted:
                try{
                    reelectLeader();
                }catch (InterruptedException e) {
                }catch (KeeperException e){
                }
        }
    }

//    //this method gets the full znode full path and znode Name
//    private String getZnodeFullPath(String znodePrefix) throws KeeperException, InterruptedException {
//        return zookeeper.create(znodePrefix,
//                new byte[]{},
//                ZooDefs.Ids.OPEN_ACL_UNSAFE,
//                CreateMode.EPHEMERAL_SEQUENTIAL);
//    }


}
