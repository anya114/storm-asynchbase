/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.utils;

import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * A fully asynchronous, thread-safe, modern HBase client.<br/>
 * Unlike the traditional HBase client (HTable),
 * this client should be instantiated only once.<br/>
 * You can use it with any number of tables at the same time.<br/>
 * The only case where you should have multiple instances is when
 * you want to use multiple different clusters at the same time.
 * </p>
 * <p>
 * This client is fully non-blocking, any blocking operation will
 * return a Deferred instance to which you can attach a Callback
 * chain that will execute when the asynchronous operation completes.
 * </p>
 * <p>
 * Every HBaseRpc passed to a method of the HBaseClient should not
 * be changed or re-used until the Deferred returned by that method
 * calls you back.<br/>
 * <b>Changing or re-using any HBaseRpc for an RPC in flight will lead
 * to unpredictable results and voids your warranty.</b><br/>
 * </p>
 * <p>
 * <b>You must shut it down gracefully by calling shutdown().
 * If you fail to do this, then all edits still buffered by the client will be lost.</b>
 * </p>
 */
public class AsyncHBaseClientFactory {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseClientFactory.class);

    private static final Map<String, HBaseClient> hBaseClientMap = new HashMap<>();


    /**
     * <p>
     * Register an HBaseClient if it does not exists,
     * return the existing instance otherwise.
     * </p>
     *
     * @param config Topology config.
     * @param name   HBaseClient to return.
     * @return Topology config key.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public synchronized static HBaseClient getHBaseClient(Map config, String name) {
        HBaseClient client = hBaseClientMap.get(name);

        if (client == null) {
            Map<String, String> conf = (Map<String, String>) config.get(name);
            if (conf == null) {
                throw new RuntimeException("Missing configuration for AsyncHBase client : " + name);
            }

            String zkQuorum = conf.get("zkQuorum");
            String zkPath = conf.get("zkPath");
            String flushInterval = conf.get("flushInterval");

            log.info("New AsyncHBase client : " + name + " (" + zkQuorum + ")");

            if (zkPath == null) {
                client = new HBaseClient(zkQuorum);
            } else {
                client = new HBaseClient(zkQuorum, zkPath);
            }

            if (flushInterval != null) {
                client.setFlushInterval(Short.parseShort(flushInterval));
            }

            hBaseClientMap.put(name, client);
        }

        return client;
    }

    /**
     * <p>
     * Gracefully shutdown an AsyncHBaseClient.<br>
     * if name is null, shutdown all registered clients.
     * </p>
     *
     * @param name Client to shutdown.
     */
    public synchronized void shutdown(String name) {
        if (name == null) {
            for (HBaseClient client : hBaseClientMap.values()) {
                client.shutdown();
            }
        } else {
            HBaseClient client = hBaseClientMap.get(name);
            if (client != null) {
                client.shutdown();
                hBaseClientMap.remove(client);
            } else {
                log.error("AsyncHBase client " + name + " not found");
            }
        }
    }
}