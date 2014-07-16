/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.bolt.mapper;

import backtype.storm.tuple.Tuple;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.PutRequest;
import storm.asynchbase.utils.serializer.AsyncHBaseSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This class configure an HBase RPC<br/>
 * It maps tuple fields to part of the request
 * and sets options
 * </p>
 * TODO: handle scan requests
 */
public class AsyncHBaseFieldMapper implements IAsyncHBaseFieldMapper {
    private Type type = Type.PUT;

    private byte[] table;
    private byte[] columnFamily;
    private byte[] columnQualifier;
    private byte[][] columnQualifiers;
    private byte[] rowKey;
    private byte[] value;
    private byte[][] values;
    private long increment = 0;
    private long timestamp = -1;

    private String tableField;
    private String columnFamilyField;
    private String columnQualifierField;
    private String columnQualifiersField;
    private List<String> columnQualifierFields;
    private String rowKeyField;
    private String valueField;
    private String valuesField;
    private List<String> valueFields;
    private String mapField;
    private String incrementField;
    private String timestampField;

    private AsyncHBaseSerializer columnFamilySerializer;
    private AsyncHBaseSerializer columnQualifierSerializer;
    private AsyncHBaseSerializer rowKeySerializer;
    private AsyncHBaseSerializer valueSerializer;

    private int versions;
    private boolean durable;
    private boolean bufferable;

    private Constructor constructor;

    /**
     * This has to be called before running the query<br/>
     * Typically by the prepare method -- To get the
     * right constructor to call to execute the rpc.<br/>
     * If not you'll get a InvalidMapperException.<br/>
     * <b>If you change the mapping you have to call
     * this method again.</b>
     */
    public void updateMapping() {
        if (this.table == null && this.tableField == null) {
            throw new InvalidMapperException("Missing table");
        }
        if (this.rowKey == null && this.rowKeyField == null) {
            throw new InvalidMapperException("Missing rowkey");
        }

        switch (this.type) {
            case PUT:
                if (this.columnFamily == null && this.columnFamilyField == null) {
                    throw new InvalidMapperException("Missing column family");
                }
                if (this.value != null || this.valueField != null) {
                    if (this.columnQualifier == null && this.columnQualifierField == null) {
                        throw new InvalidMapperException("Missing column qualifier");
                    }
                    if (this.timestamp > 0 || this.timestampField != null) {
                        this.constructor = Constructor.VALUE_WITH_TIMESTAMP;
                    } else {
                        this.constructor = Constructor.VALUE;
                    }
                    break;
                }
                if (this.values != null || this.valuesField != null || this.valueFields != null ) {
                    if (this.columnQualifiers == null && this.columnQualifierFields == null) {
                        throw new InvalidMapperException("Missing column qualifiers");
                    }
                    if (this.timestamp > 0 || this.timestampField != null) {
                        this.constructor = Constructor.VALUES_WITH_TIMESTAMP;
                    } else {
                        this.constructor = Constructor.VALUES;
                    }
                    break;
                }
                if (this.mapField != null){
                    if (this.timestamp > 0 || this.timestampField != null) {
                        this.constructor = Constructor.MAP_WITH_TIMESTAMP;
                    } else {
                        this.constructor = Constructor.MAP;
                    }
                    break;
                }
                throw new InvalidMapperException("Missing value");
            case INCR:
                this.constructor = Constructor.VALUE;
                if (this.increment <= 0 && this.incrementField == null) {
                    throw new InvalidMapperException("Missing increment amount");
                }
                if (this.columnQualifier == null && this.columnQualifierField == null) {
                    throw new InvalidMapperException("Missing column qualifier");
                }
                if (this.columnFamily == null && this.columnFamilyField == null) {
                    throw new InvalidMapperException("Missing column family");
                }
                break;
            case DELETE:
                if (this.columnQualifier != null || this.columnQualifierField != null) {
                    if (this.columnFamily == null && this.columnFamilyField == null) {
                        throw new InvalidMapperException("Missing column family");
                    }
                    if (this.timestamp > 0 || this.timestampField != null) {
                        this.constructor = Constructor.CELL_WITH_TIMESTAMP;
                    } else {
                        this.constructor = Constructor.CELL;
                    }
                    break;
                }
                if (this.columnQualifiers != null || this.columnQualifiersField != null || this.columnQualifierFields != null) {
                    if (this.columnFamily == null && this.columnFamilyField == null) {
                        throw new InvalidMapperException("Missing column family");
                    }
                    if (this.timestamp > 0 || this.timestampField != null) {
                        this.constructor = Constructor.CELLS_WITH_TIMESTAMP;
                    } else {
                        this.constructor = Constructor.CELLS;
                    }
                    break;
                }
                if (this.columnFamily != null && this.columnFamilyField != null) {
                    if (this.timestamp > 0 || this.timestampField != null) {
                        this.constructor = Constructor.FAMILY_WITH_TIMESTAMP;
                    } else {
                        this.constructor = Constructor.FAMILY;
                    }
                    break;
                }
                if (this.timestamp > 0 || this.timestampField != null) {
                    this.constructor = Constructor.ROW_WITH_TIMESTAMP;
                } else {
                    this.constructor = Constructor.ROW;
                }
                break;
            case GET:
                if (this.columnQualifier != null || this.columnQualifierField != null) {
                    if (this.columnFamily == null && this.columnFamilyField == null) {
                        throw new InvalidMapperException("Missing column family");
                    }
                    this.constructor = Constructor.CELL;
                    break;
                }
                if (this.columnQualifiers != null || this.columnQualifiersField != null || this.columnQualifierFields != null) {
                    if (this.columnFamily == null && this.columnFamilyField == null) {
                        throw new InvalidMapperException("Missing column family");
                    }
                    this.constructor = Constructor.CELLS;
                    break;
                }
                if (this.columnFamily != null && this.columnFamilyField != null) {
                    this.constructor = Constructor.FAMILY;
                    break;
                }
                this.constructor = Constructor.ROW;
                break;
            default:
                throw new InvalidMapperException("Invalid request type");
        }
    }

    /**
     * @param tuple The storm tuple to process.
     * @return PutRequest to execute.
     */
    @Override
    public PutRequest getPutRequest(Tuple tuple) {
        PutRequest req;
        switch (this.constructor) {
            case VALUE:
                req = new PutRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    this.getColumnQualifier(tuple),
                    this.getValue(tuple)
                );
                break;
            case VALUE_WITH_TIMESTAMP:
                req = new PutRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    this.getColumnQualifier(tuple),
                    this.getValue(tuple),
                    this.getTimestamp(tuple)
                );
                break;
            case VALUES:
                req = new PutRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    this.getColumnQualifiers(tuple),
                    this.getValues(tuple)
                );
                break;
            case VALUES_WITH_TIMESTAMP:
                req = new PutRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    this.getColumnQualifiers(tuple),
                    this.getValues(tuple),
                    this.getTimestamp(tuple)
                );
                break;
            case MAP:
                byte[][][] values = this.getQualifiersAndValues(tuple);
                req = new PutRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    values[0],
                    values[1]
                );
                break;
            case MAP_WITH_TIMESTAMP:
                byte[][][] values2 = this.getQualifiersAndValues(tuple);
                req = new PutRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    values2[0],
                    values2[1],
                    this.getTimestamp(tuple)
                );
                break;
            default:
                if (this.constructor == null) {
                    throw new InvalidMapperException("uninitialized mapper");
                }
                throw new InvalidMapperException("invalid field mapper for PutRequest");
        }

        if (!this.bufferable) {
            req.setBufferable(false);
        }

        if (!this.durable) {
            req.setDurable(false);
        }

        return req;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return AtomicIncrementRequest RPC to execute.
     */
    @Override
    public AtomicIncrementRequest getIncrementRequest(Tuple tuple) {
        switch (this.constructor) {
            case VALUE:
                return new AtomicIncrementRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    this.getColumnQualifier(tuple),
                    this.getIncrement(tuple)
                );
            default:
                if (this.constructor == null) {
                    throw new InvalidMapperException("uninitialized mapper");
                }
                throw new InvalidMapperException("invalid field mapper for AtomicIncrementRequest");
        }
    }

    /**
     * @param tuple The storm tuple to process.
     * @return AtomicIncrementRequest RPC to execute.
     */
    @Override
    public DeleteRequest getDeleteRequest(Tuple tuple) {
        DeleteRequest req;
        switch (this.constructor) {
            case CELL:
                req = new DeleteRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    this.getColumnQualifier(tuple)
                );
                break;
            case CELL_WITH_TIMESTAMP:
                req = new DeleteRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    this.getColumnQualifier(tuple),
                    this.getTimestamp(tuple)
                );
                break;
            case CELLS:
                req = new DeleteRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    this.getColumnQualifiers(tuple)
                );
                break;
            case CELLS_WITH_TIMESTAMP:
                req = new DeleteRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    this.getColumnQualifiers(tuple),
                    this.getTimestamp(tuple)
                );
                break;
            case FAMILY:
                req = new DeleteRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple)
                );
                break;
            case FAMILY_WITH_TIMESTAMP:
                req = new DeleteRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    this.getTimestamp(tuple)
                );
                break;
            case ROW:
                req = new DeleteRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple)
                );
                break;
            case ROW_WITH_TIMESTAMP:
                req = new DeleteRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getTimestamp(tuple)
                );
                break;
            default:
                if (this.constructor == null) {
                    throw new InvalidMapperException("uninitialized mapper");
                }
                throw new InvalidMapperException("invalid field mapper for DeleteRequest");
        }

        if (req != null && !this.durable) {
            req.setDurable(false);
        }

        if (req != null && !this.bufferable) {
            req.setBufferable(false);
        }

        return req;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return AtomicIncrementRequest RPC to execute.
     */
    @Override
    public GetRequest getGetRequest(Tuple tuple) {
        GetRequest req;
        switch (this.constructor) {
            case CELL:
                req = new GetRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple),
                    this.getColumnQualifier(tuple)
                );
                break;
            case CELLS:
                req = new GetRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple)
                );
                req.qualifiers(getColumnQualifiers(tuple));
                break;
            case FAMILY:
                req = new GetRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple),
                    this.getColumnFamily(tuple)
                );
                break;
            case ROW:
                req = new GetRequest(
                    this.getTable(tuple),
                    this.getRowKey(tuple)
                );
                break;
            default:
                if (this.constructor == null) {
                    throw new InvalidMapperException("uninitialized mapper");
                }
                throw new InvalidMapperException("invalid field mapper for GetRequest");
        }

        if (this.versions > 0) {
            req.maxVersions(this.versions);
        }

        return req;
    }

    /**
     * @return Type of the RPC to execute.
     */
    @Override
    public Type getRpcType() {
        return this.type;
    }

    /**
     * @param type Type of the RPC to execute.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setRpcType(Type type) {
        this.type = type;
        return this;
    }

    /**
     * @param table HBase table to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setTable(String table) {
        this.table = table.getBytes();
        return this;
    }

    /**
     * @param tableField Name of the tuple field containing the HBase table to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setTableField(String tableField) {
        this.tableField = tableField;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return Table name as a byte array.
     */
    public byte[] getTable(Tuple tuple) {
        if (this.table != null) {
            return this.table;
        }
        return tuple.getStringByField(this.tableField).getBytes();
    }

    /**
     * @param rowKey Rowkey to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setRowKey(Object rowKey) {
        if (this.rowKeySerializer != null) {
            this.rowKey = this.columnQualifierSerializer.serialize(rowKey);
        } else {
            this.rowKey = ((String) rowKey).getBytes();
        }

        return this;
    }

    /**
     * @param rowKeyField Name of the tuple fields containing the
     *                    row key to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setRowKeyField(String rowKeyField) {
        this.rowKeyField = rowKeyField;
        return this;
    }

    /**
     * @param serializer An AsyncHBaseSerializer to use to transform the row key to
     *                   byte array.<br/>
     *                   Note that if you used a constant value ( setRowKey )
     *                   you have to provide the serializer befor so that the serialization
     *                   is done only once.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setRowKeySerializer(AsyncHBaseSerializer serializer) {
        this.rowKeySerializer = serializer;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return Row key as a byte array.
     */
    public byte[] getRowKey(Tuple tuple) {
        if (this.rowKeyField == null) {
            return this.rowKey;
        }
        if (this.rowKeySerializer != null) {
            return this.rowKeySerializer.serialize(tuple.getValueByField(this.rowKeyField));
        }
        return tuple.getStringByField(this.rowKeyField).getBytes();
    }

    /**
     * @param columnFamily Column family to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setColumnFamily(Object columnFamily) {
        if (this.columnFamilySerializer != null) {
            this.columnFamily = this.columnFamilySerializer.serialize(columnFamily);
        } else {
            this.columnFamily = ((String) columnFamily).getBytes();
        }
        return this;
    }

    /**
     * @param columnFamilyField Name of the tuple field containing the
     *                          column family to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setColumnFamilyField(String columnFamilyField) {
        this.columnFamilyField = columnFamilyField;
        return this;
    }

    /**
     * @param serializer An AsyncHBaseSerializer to use to transform column family to
     *                   byte array.<br/>
     *                   Note that if you use a constant value ( setColumnFamily )
     *                   you have to provide the serializer before so that the serialization
     *                   is done only once.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setColumnFamilySerializer(AsyncHBaseSerializer serializer) {
        this.columnFamilySerializer = serializer;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return Column family as a byte array.
     */
    public byte[] getColumnFamily(Tuple tuple) {
        if (this.columnFamilyField == null) {
            return this.columnFamily;
        }
        if (this.columnFamilySerializer != null) {
            return this.columnFamilySerializer.serialize(tuple.getValueByField(this.columnFamilyField));
        }
        return tuple.getStringByField(this.columnFamilyField).getBytes();
    }

    /**
     * @param columnQualifier Column qualifier to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setColumnQualifier(Object columnQualifier) {
        if (this.columnQualifierSerializer != null) {
            this.columnQualifier = this.columnQualifierSerializer.serialize(columnQualifier);
        } else {
            this.columnQualifier = ((String) columnQualifier).getBytes();
        }
        return this;
    }

    /**
     * @param columnQualifierField Name of the tuple field containing the
     *                             column qualifier to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setColumnQualifierField(String columnQualifierField) {
        this.columnQualifierField = columnQualifierField;
        return this;
    }

    /**
     * @param serializer An AsyncHBaseSerializer to use to transform column qualifier to
     *                   byte array.<br/>
     *                   Note that if you used a constant value ( setColumnQualifier )
     *                   you have to provide the serializer before so that the serialization
     *                   is done only once.<br/>
     *                   Also apply if you use multiple column qualifiers.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setColumnQualifierSerializer(AsyncHBaseSerializer serializer) {
        this.columnQualifierSerializer = serializer;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return Column qualifier as a byte array.
     */
    public byte[] getColumnQualifier(Tuple tuple) {
        if (this.columnQualifierField == null) {
            return this.columnQualifier;
        }
        if (this.columnQualifierSerializer != null) {
            return this.columnQualifierSerializer.serialize(tuple.getValueByField(this.columnQualifierField));
        }
        return tuple.getStringByField(this.columnQualifierField).getBytes();
    }

    /**
     * @param columnQualifiers List of column qualifiers to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setColumnQualifiers(List<Object> columnQualifiers) {
        this.columnQualifiers = new byte[columnQualifiers.size()][];
        if (this.columnQualifierSerializer != null) {
            for (int i = 0; i < columnQualifiers.size(); i++) {
                this.columnQualifiers[i] = this.columnQualifierSerializer.serialize(columnQualifiers.get(i));
            }
        } else {
            for (int i = 0; i < columnQualifiers.size(); i++) {
                this.columnQualifiers[i] = ((String) columnQualifiers.get(i)).getBytes();
            }
        }
        return this;
    }

    /**
     * @param columnQualifierFields Name of the tuple fields containing the
     *                              column qualifiers to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setColumnQualifierFields(List<String> columnQualifierFields) {
        this.columnQualifierFields = columnQualifierFields;
        return this;
    }

    /**
     * @param columnQualifiersField Name of the tuple fields containing a
     *                              list of column qualifiers to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setColumnQualifiersField(String columnQualifiersField) {
        this.columnQualifiersField = columnQualifiersField;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return Column qualifiers as an array of byte array.
     */
    public byte[][] getColumnQualifiers(Tuple tuple) {
        if (this.columnQualifiersField != null) {
            ArrayList<Object> tupleValues = (ArrayList<Object>) tuple.getValueByField(this.columnQualifiersField);
            byte[][] qualifiers = new byte[tupleValues.size()][];
            if (this.columnQualifierSerializer != null) {
                for (int i = 0; i < tupleValues.size(); i++) {
                    qualifiers[i] = this.valueSerializer.serialize(tupleValues.get(i));
                }
            } else {
                for (int i = 0; i < tupleValues.size(); i++) {
                    qualifiers[i] = ((String)tupleValues.get(i)).getBytes();
                }
            }
            return qualifiers;
        } else if (this.columnQualifierFields != null) {
            byte[][] qualifiers = new byte[this.columnQualifierFields.size()][];
            if (this.columnQualifierSerializer != null) {
                for (int i = 0; i < this.columnQualifierFields.size(); i++) {
                    qualifiers[i] = this.columnQualifierSerializer.serialize(tuple.getValueByField(this.columnQualifierFields.get(i)));
                }
            } else {
                for (int i = 0; i < this.columnQualifierFields.size(); i++) {
                    qualifiers[i] = tuple.getStringByField(this.columnQualifierFields.get(i)).getBytes();
                }
            }
            return qualifiers;
        } else {
            return this.columnQualifiers;
        }
    }

    /**
     * @param value Cell value to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setValue(Object value) {
        if (this.valueSerializer != null) {
            this.value = this.valueSerializer.serialize(value);
        } else {
            this.value = ((String) value).getBytes();
        }
        return this;
    }

    /**
     * @param valueField Name of the tuple fields containing the
     *                   cell value to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setValueField(String valueField) {
        this.valueField = valueField;
        return this;
    }

    /**
     * @param serializer An AsyncHBaseSerializer to use to transform the cell value to
     *                   byte array.<br/>
     *                   Note that if you used a constant value ( setValue )
     *                   you have to provide the serializer before so that the serialization
     *                   is done only once.<br/>
     *                   Also apply if you use multiple qualifiers/values.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setValueSerializer(AsyncHBaseSerializer serializer) {
        this.valueSerializer = serializer;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return Cell value as a byte array.
     */
    public byte[] getValue(Tuple tuple) {
        if (this.valueField == null) {
            return this.value;
        }
        if (this.valueSerializer != null) {
            return this.valueSerializer.serialize(tuple.getValueByField(this.valueField));
        }
        return tuple.getStringByField(this.valueField).getBytes();
    }


    /**
     * Note : Values are mapped to qualifiers in order.
     * @param values List of cell values to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setValues(List<Object> values) {
        this.values = new byte[values.size()][];
        if (this.valueSerializer != null) {
            for (int i = 0; i < values.size(); i++) {
                this.values[i] = this.valueSerializer.serialize(values.get(i));
            }
        } else {
            for (int i = 0; i < values.size(); i++) {
                this.values[i] = ((String) values.get(i)).getBytes();
            }
        }
        return this;
    }

    /**
     * Note : Values are mapped to qualifiers in order.
     * @param valuesField Name of the tuple field containing a
     *                    list of values.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setValuesField(String valuesField) {
        this.valuesField = valuesField;
        return this;
    }

    /**
     * Note : Values are mapped to qualifiers in order.
     * @param valueFields Name of the tuple fields containing the
     *                    cell values to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setValueFields(List<String> valueFields) {
        this.valueFields = valueFields;
        return this;
    }

    /**
     * Note : Values are mapped to qualifiers in order.
     * @param tuple The storm tuple to process.
     * @return Cell values as an array of byte array.
     */
    public byte[][] getValues(Tuple tuple) {
        if (this.valuesField != null) {
            ArrayList<Object> tupleValues = (ArrayList<Object>) tuple.getValueByField(this.valuesField);
            byte[][] values = new byte[tupleValues.size()][];
            if (this.valueSerializer != null) {
                for (int i = 0; i < tupleValues.size(); i++) {
                    values[i] = this.valueSerializer.serialize(tupleValues.get(i));
                }
            } else {
                for (int i = 0; i < tupleValues.size(); i++) {
                    values[i] = ((String)tupleValues.get(i)).getBytes();
                }
            }
            return values;
        } else if( this.valueFields != null) {
            byte[][] values = new byte[this.valueFields.size()][];
            if (this.valueSerializer != null) {
                for (int i = 0; i < this.valueFields.size(); i++) {
                    values[i] = this.valueSerializer.serialize(tuple.getValueByField(this.valueFields.get(i)));
                }
            } else {
                for (int i = 0; i < this.valueFields.size(); i++) {
                    values[i] = tuple.getStringByField(this.valueFields.get(i)).getBytes();
                }
            }
            return values;
        } else {
            return this.values;
        }
    }

    /**
     * @param map ColumnQualifiers / values map.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setMap(Map<Object,Object> map) {
        this.columnQualifiers = new byte[map.size()][];
        this.values = new byte[map.size()][];
        int i = 0;
        for (Map.Entry<Object,Object> item : map.entrySet()){
            if (this.columnQualifierSerializer != null) {
                columnQualifiers[i] = this.columnQualifierSerializer.serialize(item.getKey());
            } else {
                columnQualifiers[i] = ((String)item.getKey()).getBytes();
            }
            if (this.valueSerializer != null) {
                values[i] = this.valueSerializer.serialize(item.getValue());
            } else {
                values[i] = ((String)item.getValue()).getBytes();
            }
            i++;
        }
        return this;
    }

    /**
     * @param mapField Name of the tuple field containing a
     *                 columnQualifier / value Map.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setMapField(String mapField) {
        this.mapField = mapField;
        return this;
    }


    /**
     * @param tuple The storm tuple to process.
     * @return index 0 - Array of byte array of qualifiers
     *         index 1 - Array of byte array of values
     */
    public byte[][][] getQualifiersAndValues(Tuple tuple) {
        byte[][][] qualifiersAndValues = new byte[2][][];
        Map<Object,Object> map = (Map<Object,Object>) tuple.getValueByField(this.mapField);
        qualifiersAndValues[0] = new byte[map.size()][];
        qualifiersAndValues[1] = new byte[map.size()][];
        int i = 0;
        for (Map.Entry<Object,Object> item : map.entrySet()){
            if (this.columnQualifierSerializer != null) {
                qualifiersAndValues[0][i] = this.columnQualifierSerializer.serialize(item.getKey());
            } else {
                qualifiersAndValues[0][i] = ((String)item.getKey()).getBytes();
            }
            if (this.valueSerializer != null) {
                qualifiersAndValues[1][i] = this.valueSerializer.serialize(item.getValue());
            } else {
                qualifiersAndValues[1][i] = ((String)item.getValue()).getBytes();
            }
            i++;
        }
        return qualifiersAndValues;
    }

    /**
     * @param increment Increment amount to do.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setIncrement(long increment) {
        this.increment = increment;
        return this;
    }

    /**
     * @param incrementField Name of the tuple field containing the
     *                       Increment amount to do.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setIncrementField(String incrementField) {
        this.incrementField = incrementField;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return Increment to do.
     */
    public long getIncrement(Tuple tuple) {
        if (this.incrementField == null) {
            return this.increment;
        }
        return tuple.getLongByField(this.incrementField);
    }

    /**
     * @param timestamp Timestamp to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * @param timestampField Name of the tuple field containing the .
     *                       timestamp to use.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setTimestampField(String timestampField) {
        this.timestampField = timestampField;
        this.timestamp = -2;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return The timestamp to use.
     */
    public long getTimestamp(Tuple tuple) {
        if (this.timestampField == null) {
            return this.timestamp;
        }
        return tuple.getLongByField(this.timestampField);
    }

    /**
     * @param maxVersions The maximum number of versions to return for each cell read.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setMaxVersion(int maxVersions) {
        this.versions = maxVersions;
        return this;
    }

    /**
     * <p>
     * Some methods or RPC types take a durable argument.
     * When an edit requests to be durable, the success of the RPC
     * <b>guarantees that the edit is safely and durably stored by HBase</b>
     * and won't be lost. In case of server failures, the edit won't be
     * lost although it may become momentarily unavailable.
     * Setting the durable argument to false makes the operation complete faster
     * (and puts a lot less strain on HBase), but removes this durability guarantee.
     * In case of a server failure, the edit may (or may not) be lost forever.
     * When in doubt, leave it to true (default).
     * Setting it to false is useful in cases where data-loss is acceptable,
     * e.g. during batch imports (where you can re-run the whole import in case of a failure),
     * or when you intend to do statistical analysis on the data
     * Bear in mind that this durability guarantee holds only once the RPC has completed successfully.
     * Any edit temporarily buffered on the client side or in-flight will be lost
     * if the client itself crashes. You can control how much buffering is done by
     * the client by settings the client flushInterval. and you can force-flush the buffered edits
     * by calling flush(). When you're done using HBase, you must not just give up
     * your reference to your HBaseClient, you must shut it down gracefully by calling shutdown().
     * If you fail to do this, then all edits still buffered by the client will be lost.
     * </p>
     *
     * @param durable The durability setting of this edit.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setDurable(boolean durable) {
        this.durable = durable;
        return this;
    }

    /**
     * @param bufferable Whether or not this RPC is can be buffered on the client side.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseFieldMapper setBufferable(boolean bufferable) {
        this.bufferable = bufferable;
        return this;
    }

    /**
     * <p>
     * Initialize the mapper and the serializers if any.
     * </p>
     *
     * @param conf Topology configuration.
     */
    @Override
    public void prepare(Map conf) {
        this.updateMapping();

        if (this.columnFamilySerializer != null) {
            this.columnFamilySerializer.prepare(conf);
        }
        if (this.columnQualifierSerializer != null) {
            this.columnQualifierSerializer.prepare(conf);
        }
        if (this.rowKeySerializer != null) {
            this.rowKeySerializer.prepare(conf);
        }
        if (this.valueSerializer != null) {
            this.valueSerializer.prepare(conf);
        }
    }

    /**
     * <p>
     * The constructor to use when executing the request
     * Computed at initialization time to save coputation at
     * request's runtime. If you change the mapping you have to
     * call updateMapping again.
     * </p>
     */
    private enum Constructor {
        VALUE,
        VALUE_WITH_TIMESTAMP,
        VALUES,
        VALUES_WITH_TIMESTAMP,
        MAP,
        MAP_WITH_TIMESTAMP,
        CELL,
        CELL_WITH_TIMESTAMP,
        DYNAMIC_CELL,
        DYNAMIC_CELL_WITH_TIMESTAMP,
        CELLS,
        CELLS_WITH_TIMESTAMP,
        FAMILY,
        FAMILY_WITH_TIMESTAMP,
        ROW,
        ROW_WITH_TIMESTAMP
    }

    /**
     * <p>
     * You'll get this exception if you forgot to initialize the mapper
     * or if the settings provided does not permit to find a valid
     * constructor to use to execute the request.
     * </p>
     */
    class InvalidMapperException extends RuntimeException {
        public InvalidMapperException(String message) {
            super(type + " request : " + message);
        }
    }
}
