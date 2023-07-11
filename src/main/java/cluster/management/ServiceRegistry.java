package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher{
    private static final String REGISTRY_ZNODE = "/service_registry";
    private final ZooKeeper zookeeper;
    private String currentZnode = null;
    private List<String> allServiceAddresses = null;

    public ServiceRegistry(ZooKeeper zookeeper){
        this.zookeeper = zookeeper;
        createServiceRegistryZnode();
    }

    //logic to join the cluster
    public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
        this.currentZnode = zookeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("REGISTERED TO SERVICE REGISTRY");
    }

    //initial call to update addresses.
    public void registerForUpdates(){
        try {
            updateAddresses();
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        }
    }

    //get the most up to date list of addresses
    public synchronized List<String> getAllServiceAddresses() throws InterruptedException, KeeperException {
        if(allServiceAddresses == null){
            updateAddresses();
        }

        return allServiceAddresses;
    }

    //unregister node from the cluster
    public void unregisterFromCluster()  {
        try {
            if (currentZnode != null && zookeeper.exists(currentZnode, false) != null) {
                zookeeper.delete(currentZnode, -1);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private void createServiceRegistryZnode(){
        try {
            if(zookeeper.exists(REGISTRY_ZNODE, false) == null){
                zookeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //logic to get updates about nodes leaving or joining the cluster
    private synchronized void updateAddresses() throws InterruptedException, KeeperException {
        List<String> workerZnodes = zookeeper.getChildren(REGISTRY_ZNODE, this);

        List<String> addresses = new ArrayList<>(workerZnodes.size());

        for(String workerZnode : workerZnodes){

            String workerZnodeFullPath = REGISTRY_ZNODE + "/" + workerZnode;
            Stat stat = zookeeper.exists(workerZnodeFullPath, false);
            if(stat == null){
                continue;
            }

            byte[] addressBytes = zookeeper.getData(workerZnodeFullPath, false, stat);

            String address = new String(addressBytes);
            addresses.add(address);


        }

        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The cluster addresses are : " + this.allServiceAddresses);
    }


    @Override
    public void process(WatchedEvent event) {
        try {
            updateAddresses();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
