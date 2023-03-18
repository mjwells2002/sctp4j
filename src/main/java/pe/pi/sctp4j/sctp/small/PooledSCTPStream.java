package pe.pi.sctp4j.sctp.small;

import pe.pi.sctp4j.sctp.SCTPMessage;
import pe.pi.sctp4j.sctp.SCTPStream;
import pe.pi.sctp4j.sctp.dataChannel.DECP.DCOpen;
import pe.pi.sctp4j.sctp.messages.DataChunk;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;

public class PooledSCTPStream extends SCTPStream {
    private HashMap<Integer, SCTPMessage> undeliveredMessages = new HashMap();
    private PooledAssociation pooledAssociation;
    private ExecutorService executorService;

    public PooledSCTPStream(PooledAssociation pooledAssociation, int id, ExecutorService executorService) {
        super(pooledAssociation,id);
        this.pooledAssociation = pooledAssociation;
        this.executorService = executorService;
    }

    @Override
    public void delivered(DataChunk dataChunk) {
        int flags = dataChunk.getFlags();
        if ((flags & 1) > 0) {
            int ssn = dataChunk.getSSeqNo();
            SCTPMessage st = this.undeliveredMessages.remove(ssn);
            if (st != null) {
                st.acked();
            }
        }
    }

    @Override
    public void send(String s) throws Exception {
        SCTPMessage message = pooledAssociation.makeMessage(s,this);
        undeliveredMessages.put(message.getSeq(),message);
        pooledAssociation.sendAndBlock(message);
    }

    @Override
    public void send(byte[] bytes) throws Exception {
        SCTPMessage message = pooledAssociation.makeMessage(bytes,this);
        undeliveredMessages.put(message.getSeq(),message);
        pooledAssociation.sendAndBlock(message);
    }

    @Override
    public void send(DCOpen dcOpen) throws Exception {
        SCTPMessage message = pooledAssociation.makeMessage(dcOpen, this);
        undeliveredMessages.put(message.getSeq(), message);
        pooledAssociation.sendAndBlock(message);
    }

    @Override
    public void deliverMessage(SCTPMessage sctpMessage) {
        executorService.execute(sctpMessage);
    }

    protected void alOnDCEPStream(SCTPStream _stream, String label, int _pPid) throws Exception {
        executorService.execute(() -> {
            try {
                super.alOnDCEPStream(_stream, label, _pPid);
            } catch (Exception ignored) {

            }
        });
    }

    @Override
    public boolean idle() {
        return undeliveredMessages.isEmpty();
    }

}
