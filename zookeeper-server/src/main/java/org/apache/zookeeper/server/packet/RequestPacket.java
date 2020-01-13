package org.apache.zookeeper.server.packet;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.jute.Record;
import org.apache.zookeeper.proto.RequestHeader;

/**
 * Abstracts a request payload
 */
public interface RequestPacket {
    /**
     * Return a {@code RequestPacket} from a serialized byte buffer
     *
     * @param buffer serialized byte buffer
     * @return request packet
     */
    static RequestPacket fromBytes(ByteBuffer buffer) {
        return new ByteBufferRequestPacket(buffer);
    }

    /**
     * Return a {@code RequestPacket} from a serialized bytes
     *
     * @param buffer serialized bytes
     * @return request packet
     */
    static RequestPacket fromBytes(byte[] buffer) {
        return fromBytes(ByteBuffer.wrap(buffer));
    }

    /**
     * Read and return the request header
     *
     * @return request header
     * @throws IOException read errors
     */
    RequestHeader readRequestHeader() throws IOException;

    /**
     * Read the reocrd of the given type. If the enclosed/serialized record does not
     * match the requested type the behavior is undefined.
     *
     * @param recordClazz record type
     * @return record
     * @throws IOException read errors
     */
    <T extends Record> T readRecord(Class<T> recordClazz) throws IOException;

    /**
     * Returns a view of this request packet that does not change the internal buffer position. Can be used
     * to read a record without affecting the parent request packet.
     *
     * @return temp view
     */
    RequestPacket tempRequestPacket();

    /**
     * Serialize this request packet into a byte array so that it can be sent to another ZooKeeper instance
     *
     * @return serialized request packet
     */
    byte[] toByteRequest();

    /**
     * Return the internal length of the request packet
     *
     * @return length
     */
    int length();

    /**
     * Reset any internal buffers to their original positions
     */
    void reset();

    /**
     * Append a diagnostic for logging purposes
     *
     * @param sb string builder to append to
     */
    void appendDiagnostic(StringBuilder sb);
}
