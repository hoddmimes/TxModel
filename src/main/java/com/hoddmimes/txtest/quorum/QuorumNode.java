package com.hoddmimes.txtest.quorum;

import com.hoddmimes.txtest.generated.ipc.messages.QuorumStatusHeartbeat;
import com.hoddmimes.txtest.server.ServerRole;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class QuorumNode {

        final int mNodeId;
        final boolean mPreferredPrimary;
        final Logger mLogger;
        private volatile ServerRole mRole = ServerRole.UNKNOWN;
        private volatile boolean mIsConnected = false;


        public QuorumNode(int id, boolean preferredPrimary) {
            this.mNodeId = id;
            this.mLogger = LogManager.getLogger( this.getClass().getSimpleName() + "-id-" + String.valueOf(id));
            this.mPreferredPrimary = preferredPrimary;
        }

        public synchronized int getNodeId() { return mNodeId; }
        public boolean isPreferredPrimary() { return mPreferredPrimary; }
        public synchronized ServerRole getRole() { return mRole; }
        public synchronized void setState(ServerRole pRole) { this.mRole = pRole; }
        public synchronized void setConnected(boolean pIsConnected) { this.mIsConnected = pIsConnected; }
        public boolean isIsConnected() { return mIsConnected; }

        public void statusHeartbeat(QuorumStatusHeartbeat message) {
            if (mRole == ServerRole.UNKNOWN)  {
                if (!mIsConnected) {
                    mIsConnected = true;
                    mLogger.info("Quorum Node {} is now connect (Quorum Status Bdx)", mNodeId);
                }
                mRole = ServerRole.valueOf(message.getServerRole());
                mLogger.info("Quorum Node {} updated state to: {}", mNodeId, mRole);
            }
            mLogger.trace("Quorum Status Heartbeat BDX from node: {} state: {}", mNodeId, mRole);
        }

        @Override
        public String toString() {
            return String.format("QNode{id=%d, preferredPrimary=%b, state=%s}",
                    mNodeId, mPreferredPrimary, mRole);
        }

}
