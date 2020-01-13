package org.apache.zookeeper.server.packet;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.server.ByteBufferInputStream;

class ByteBufferRequestPacket implements RequestPacket {
    private ByteBuffer buffer;

    ByteBufferRequestPacket(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public RequestHeader readRequestHeader() throws IOException {
        // We have the request, now process and setup for next
        InputStream bais = new ByteBufferInputStream(buffer);
        BinaryInputArchive bia = BinaryInputArchive.getArchive(bais);
        RequestHeader h = new RequestHeader();
        h.deserialize(bia, "header");
        // Through the magic of byte buffers, txn will now be
        // pointing
        // to the start of the txn
        buffer = buffer.slice();
        return h;
    }

    @Override
    public <T extends Record> T readRecord(Class<T> recordClazz) throws IOException {
        return internalReadRecord(recordClazz, buffer);
    }

    @Override
    public RequestPacket tempRequestPacket() {
        return new ByteBufferRequestPacket(buffer.slice());
    }

    @Override
    public String toString() {
        if (buffer.remaining() >= 4) {
            // make sure we don't mess with request itself
            ByteBuffer rbuf = buffer.asReadOnlyBuffer();
            rbuf.clear();
            int pathLen = rbuf.getInt();
            // sanity check
            if (pathLen >= 0 && pathLen < 4096 && rbuf.remaining() >= pathLen) {
                byte[] b = new byte[pathLen];
                rbuf.get(b);
                return new String(b);
            }
        }
        return "n/a";
    }

    @Override
    public int length() {
        return buffer.limit();
    }

    @Override
    public void reset() {
        buffer.rewind();
    }

    @Override
    public void appendDiagnostic(StringBuilder sb) {
        buffer.rewind();
        while (buffer.hasRemaining()) {
            sb.append(Integer.toHexString(buffer.get() & 0xff));
        }
    }

    @Override
    public byte[] toByteRequest() {
        byte[] b = new byte[buffer.limit()];
        buffer.get(b);
        return b;
    }

    private <T extends Record> T internalReadRecord(Class<T> recordClazz, ByteBuffer byteBuffer) throws IOException {
        try {
            T record = recordClazz.newInstance();
            ByteBufferInputStream.byteBuffer2Record(byteBuffer, record);
            return record;
        } catch (Exception e) {
            throw new IOException("newInstance() failed for: " + recordClazz, e);
        }
    }
}
