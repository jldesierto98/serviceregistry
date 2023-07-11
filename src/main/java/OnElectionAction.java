import cluster.management.OnElectionCallback;
import cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;

import java.net.Inet4Address;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {

    private final ServiceRegistry serviceRegistry;
    private final int port;

    public OnElectionAction(ServiceRegistry serviceRegistry, int port){
        this.serviceRegistry = serviceRegistry;
        this.port = port;
    }

    @Override
    public void onElectedToBeLeader() {
        //
        serviceRegistry.unregisterFromCluster();
        serviceRegistry.registerForUpdates();
    }

    @Override
    public void onWorker() {
        try{
            String currentServerAddress = String.format("http://%s:%d", Inet4Address.getLocalHost().getCanonicalHostName(),
                    port);

            serviceRegistry.registerToCluster(currentServerAddress);
        }catch(UnknownHostException e){

        }catch(InterruptedException e){

        }catch (KeeperException e){

        }

    }
}
