/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.trident.mapper;


import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.PutRequest;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.util.Map;

/**
 * <p>
 * This interface configure an HBase RPC<br/>
 * It maps tuple fields to part of the request
 * and sets options
 * </p>
 * TODO: handle scan requests
 */
public interface IAsyncHBaseTridentFieldMapper extends Serializable {

    /**
     * @return Type of the RPC to execute.
     */
    Type getRpcType();

    /**
     * @param tuple The storm tuple to process.
     * @return PutRequest to execute.
     */
    PutRequest getPutRequest(TridentTuple tuple);

    /**
     * @param tuple The storm tuple to process.
     * @return AtomicIncrementRequest RPC to execute.
     */
    AtomicIncrementRequest getIncrementRequest(TridentTuple tuple);

    /**
     * @param tuple The storm tuple to process.
     * @return AtomicIncrementRequest RPC to execute.
     */
    DeleteRequest getDeleteRequest(TridentTuple tuple);

    /**
     * @param tuple The storm tuple to process.
     * @return AtomicIncrementRequest RPC to execute.
     */
    GetRequest getGetRequest(TridentTuple tuple);

    /**
     * <p>
     * Initialize the mapper.
     * </p>
     *
     * @param conf Topology configuration.
     */
    void prepare(Map conf);

    /**
     * <p>
     * Available RPC types
     * </p>
     */
    public enum Type {
        PUT, INCR, DELETE, GET
    }

}