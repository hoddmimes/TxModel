package com.hoddmimes.txtest.quorum;

import com.hoddmimes.txtest.generated.ipc.messages.QuorumVoteRequest;
import com.hoddmimes.txtest.server.ServerRole;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class QuorumService {
    final List<QuorumNode> mNodes = new ArrayList<>();
    private final Logger mLogger;
    private final long mVoteInterval;

    private long firstSavedVoteRequest = 0L;
    private Map<Integer, QuorumVoteRequest> voteRequests = new HashMap<>();

    public QuorumService(long voteIntervalMs, Logger logger) {
        this.mVoteInterval = voteIntervalMs;
        this.mLogger = logger;
    }

    public QuorumNode getPrimary() {
        return mNodes.stream().filter(n -> (n.getRole() == ServerRole.PRIMARY)).findFirst().orElse(null);
    }
    public QuorumNode getStandby() {
        return mNodes.stream().filter(n -> (n.getRole() == ServerRole.STANDBY)).findFirst().orElse(null);
    }



    public boolean validateVoteRequests() {
        // In a failover situation the primary will not be connected and will not provide any vote requests
        if (voteRequests.size() == 1) {
            QuorumVoteRequest voteRequest = voteRequests.values().stream().findFirst().orElse(null);
            QuorumNode tNode = this.getNode(voteRequest.getNodeId());


            if ((voteRequest.getWannabeRole() == ServerRole.PRIMARY.getValue()) &&
                    (tNode.getRole() == ServerRole.STANDBY) &&
                    (voteRequest.getCurrentRole() == ServerRole.STANDBY.getValue())) {
                // Failover from primary to standby
                for (QuorumNode quorumNode : mNodes) {
                    if (tNode.getNodeId() == quorumNode.getNodeId()) {
                        quorumNode.setState(ServerRole.PRIMARY);
                    } else {
                        quorumNode.setState(ServerRole.STANDBY);
                    }
                    mLogger.info("[VOTE] SET STATE {} for node {} (primary/standby failover)");
                }
                return true;
            }
        }


        if (voteRequests.size() != mNodes.size()) {
            return false;
        }

        List<QuorumVoteRequest> sortedVotes = voteRequests.values().stream()
                .sorted(Comparator.comparingLong(QuorumVoteRequest::getCurrentSeqno))
                .collect(Collectors.toList());

        Collections.reverse(sortedVotes);

        boolean allSeqNoZero = sortedVotes.stream().allMatch(vr -> vr.getCurrentSeqno() == 0L);
        boolean firstTwoEqualSeqNo = sortedVotes.size() > 1 &&
                sortedVotes.get(0).getCurrentSeqno() == sortedVotes.get(1).getCurrentSeqno();

        if (allSeqNoZero || firstTwoEqualSeqNo) {
            mNodes.forEach(n -> {
                n.setState(n.isPreferredPrimary() ? ServerRole.PRIMARY : ServerRole.STANDBY);
                mLogger.info("[VOTE] SET STATE {} for node {} (preferred-primary: {}, seqno: 0 or equal)",
                        n.getRole(), n,  n.isPreferredPrimary());
            });
            return true;
        }

        for (int i = 0; i < sortedVotes.size(); i++) {
            QuorumVoteRequest voteRequest = sortedVotes.get(i);
            QuorumNode quorumNode = getNode(voteRequest.getNodeId());
            if (quorumNode != null) {
                quorumNode.setState(i == 0 ? ServerRole.PRIMARY : ServerRole.STANDBY);
                mLogger.info("VOTE: SET STATE {} for node {} (seqno: {})",  quorumNode.getRole(), quorumNode, voteRequest.getCurrentSeqno());
            }
        }
        return true;
    }

    public boolean addVoteRequestAndCheckIfExpired(QuorumVoteRequest voteRequest) {
        long now = System.currentTimeMillis();

        // Have the voting lasted longer then voting interval, if so clear and start all over again
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


    public void addNode(int id, String ip, int port, boolean preferredPrimary) {
        mNodes.add(new QuorumNode(id, preferredPrimary));
    }

    QuorumNode getNode(int nodeId) {
        return mNodes.stream().filter(n -> n.getNodeId() == nodeId).findFirst().orElse(null);
    }


}