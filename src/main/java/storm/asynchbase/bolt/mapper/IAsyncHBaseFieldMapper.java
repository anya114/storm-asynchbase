/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.bolt.mapper;

import backtype.storm.tuple.Tuple;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.PutRequest;

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
public interface IAsyncHBaseFieldMapper extends Serializable {

    /**
     * @return Type of the RPC to execute.
     */
    Type getRpcType();

    /**
     * @param tuple The storm tuple to process.
     * @return PutRequest to execute.
     */
    PutRequest getPutRequest(Tuple tuple);

    /**
     * @param tuple The storm tuple to process.
     * @return AtomicIncrementRequest RPC to execute.
     */
    AtomicIncrementRequest getIncrementRequest(Tuple tuple);

    /**
     * @param tuple The storm tuple to process.
     * @return AtomicIncrementRequest RPC to execute.
     */
    DeleteRequest getDeleteRequest(Tuple tuple);

    /**
     * @param tuple The storm tuple to process.
     * @return AtomicIncrementRequest RPC to execute.
     */
    GetRequest getGetRequest(Tuple tuple);

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