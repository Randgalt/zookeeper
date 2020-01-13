package org.apache.zookeeper.rpc;

import org.apache.jute.Record;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.server.packet.RequestPacket;

class RpcRequestPacket implements RequestPacket {
    private final RequestHeader requestHeader;
    private final Record record;

    RpcRequestPacket(RequestHeader requestHeader, Record record) {
        this.requestHeader = requestHeader;
        this.record = record;
    }

    @Override
    public RequestHeader readRequestHeader() {
        return requestHeader;
    }

    @Override
    public <T extends Record> T readRecord(Class<T> recordClazz) {
        return recordClazz.cast(record);
    }

    @Override
    public RequestPacket tempRequestPacket() {
        return this;
    }

    @Override
    public byte[] toByteRequest() {
        throw new UnsupportedOperationException();  // TODO
    }

    @Override
    public int length() {
        return 0;  // TODO
    }

    @Override
    public void reset() {
        // NOP
    }

    @Override
    public void appendDiagnostic(StringBuilder sb) {
        // TODO
    }
}
