package com.hoddmimes.txtest.quorum;

import com.hoddmimes.txtest.generated.ipc.messages.QuorumHeartbeat;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumVoteRequest;
import com.hoddmimes.txtest.server.ServerState;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class QuorumService {
    final List<QNode> mNodes = new ArrayList<>();
    final String mServiceName;
    private final Logger mLogger;
    private final long mVoteInterval;

    private long firstSavedVoteRequest = 0L;
    private Map<Integer, QuorumVoteRequest> voteRequests = new HashMap<>();

    public QuorumService(String serviceName, long voteIntervalMs, Logger logger) {
        this.mServiceName = serviceName;
        this.mVoteInterval = voteIntervalMs;
        this.mLogger = logger;
    }

    public boolean validateVoteRequests() {
        if (voteRequests.size() != mNodes.size()) {
            return false;
        }

        List<QuorumVoteRequest> sortedVotes = voteRequests.values().stream()
                .sorted(Comparator.comparingLong(QuorumVoteRequest::getCurrentSeqno))
                .collect(Collectors.toList());

        boolean allSeqNoZero = sortedVotes.stream().allMatch(vr -> vr.getCurrentSeqno() == 0L);
        boolean firstTwoEqualSeqNo = sortedVotes.size() > 1 &&
                sortedVotes.get(0).getCurrentSeqno() == sortedVotes.get(1).getCurrentSeqno();

        if (allSeqNoZero || firstTwoEqualSeqNo) {
            mNodes.forEach(n -> {
                n.setmState(n.ismPreferredPrimary() ? ServerState.PRIMARY : ServerState.STANDBY);
                mLogger.info("VOTE: set {} as {} (preferred-primary: {}, seqno: 0)",
                        n, n.getmState(), n.ismPreferredPrimary());
            });
            return true;
        }

        for (int i = 0; i < sortedVotes.size(); i++) {
            QuorumVoteRequest vote = sortedVotes.get(i);
            QNode node = getNode(vote.getNodeId());
            if (node != null) {
                node.setmState(i == 0 ? ServerState.PRIMARY : ServerState.STANDBY);
                mLogger.info("VOTE: SET STATE {} as {} (seqno: {})", node, node.getmState(), vote.getCurrentSeqno());
            }
        }
        return true;
    }

    public boolean addVoteRequestAndCheckIfExpired(QuorumVoteRequest voteRequest) {
        long now = System.currentTimeMillis();
        if (firstSavedVoteRequest != 0 && (now - firstSavedVoteRequest) >= mVoteInterval) {
            voteRequests.clear();
            firstSavedVoteRequest = 0L;
            mLogger.trace("VOTE: Vote request process expired, restarting.");
            return true;
        }
        if (firstSavedVoteRequest == 0L) {
            firstSavedVoteRequest = now;
            voteRequests.clear();
        }
        voteRequests.put(voteRequest.getNodeId(), voteRequest);
        return false;
    }

    public ServerState findNodeState(int nodeId) {
        QNode node = getNode(nodeId);
        if (node == null) {
            return ServerState.UNKNOWN;
        }
        if (node.getmState() != ServerState.UNKNOWN) {
            return node.getmState();
        }
        return mNodes.stream().anyMatch(n -> n.getmState() == ServerState.PRIMARY) ?
                ServerState.STANDBY : ServerState.UNKNOWN;
    }

    public void addNode(int id, String ip, int port, boolean preferredPrimary) {
        mNodes.add(new QNode(id, ip, port, preferredPrimary));
    }

    QNode getNode(int nodeId) {
        return mNodes.stream().filter(n -> n.mId == nodeId).findFirst().orElse(null);
    }

    class QNode {
         final String mIP;
         final int mPort;
         final int mId;
         final boolean mPreferredPrimary;
         ServerState mState = ServerState.UNKNOWN;
         boolean isConnected = false;
         long mLatestHeartbeat = System.currentTimeMillis();

        public QNode(int id, String ip, int port, boolean preferredPrimary) {
            this.mId = id;
            this.mIP = ip;
            this.mPort = port;
            this.mPreferredPrimary = preferredPrimary;
        }

        public int getId() { return mId; }
        public boolean ismPreferredPrimary() { return mPreferredPrimary; }
        public ServerState getmState() { return mState; }
        public void setmState(ServerState mState) { this.mState = mState; }

        public void heartbeat(QuorumHeartbeat message) {
            mLatestHeartbeat = System.currentTimeMillis();
            if (!isConnected) {
                isConnected = true;
                mLogger.info("Node {} ({}) is now connected", mId, mIP);
            }
            if (mState == ServerState.UNKNOWN) {
                mState = ServerState.valueOf(message.getServerState());
                mLogger.info("Node {} ({}) updated state to: {}", mId, mIP, mState);
            }
            if (mState.getValue() != message.getServerState()) {
                String errorMsg = String.format("Node %d (%s) inconsistent state: current=%s, HB=%s",
                        mId, mIP, mState, ServerState.valueOf(message.getServerState()));
                mLogger.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
            mLogger.trace("heartbeat from node: {} state: {}", mId, mState );
        }

        @Override
        public String toString() {
            return String.format("QNode{id=%d, ip=%s, port=%d, preferredPrimary=%b, state=%s}",
                    mId, mIP, mPort, mPreferredPrimary, mState);
        }
    }
}