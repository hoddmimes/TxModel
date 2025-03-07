package com.hoddmimes.txtest.server;

import com.google.gson.JsonObject;
import com.hoddmimes.txtest.aux.TxCntx;
import com.hoddmimes.txtest.aux.txlogger.TxLogger;
import com.hoddmimes.txtest.aux.txlogger.TxlogWriter;
import com.hoddmimes.txtest.generated.fe.messages.RequestMessage;
import com.hoddmimes.txtest.generated.fe.messages.UpdateMessage;
import com.hoddmimes.txtest.generated.fe.messages.UpdateResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;


public class AssetController
{
    private HashMap<Integer,Asset> mAssetMap;
    private Logger mLogger = LogManager.getLogger( AssetController.class);
    private TxLogger txLogger;
    private TxlogWriter txWriter;

    public AssetController(JsonObject jConfiguration ) {
        int tNumberOfAssets = jConfiguration.get("number_of_assets").getAsInt();

        JsonObject jTxLoggerConfig = jConfiguration.get("service").getAsJsonObject().get("tx_logging").getAsJsonObject();
        txLogger = new TxLogger( jTxLoggerConfig );
        txWriter = txLogger.getWriter();

        mAssetMap = new HashMap<>();
        for (int i = 0; i < tNumberOfAssets; i++) {
            Asset a = new Asset((i+1));
            mAssetMap.put( a.getAssetId(), a );
        }
    }

    private UpdateResponse createUpdateResponse( int pAssetId, int pRqstId, boolean pStatusOk, String pStatusText) {
        UpdateResponse rsp = new UpdateResponse();
        rsp.setStatusText(pStatusText);
        rsp.setRequestId( pRqstId );
        rsp.setStatusOk( pStatusOk );
        return rsp;
    }



    public void processClientMessage(TxCntx pTxCntx) {
        RequestMessage tRqstMsg = (RequestMessage) pTxCntx.mRequest;
        int tAssetId = tRqstMsg.getAssetId();
        Asset tAsset = mAssetMap.get( tAssetId );

        if (tAsset == null) {
            pTxCntx.addTimestamp("start to send response");
            pTxCntx.sendResponse( createUpdateResponse( tAssetId, tRqstMsg.getRequestId(), false, "Unknown asset id (" + tAssetId + ")") );
            pTxCntx.addTimestamp("response sent");
            mLogger.warn("Unknown asset id (" + tAssetId + ")");
            return;
        }
        if (tRqstMsg instanceof UpdateMessage) {
            UpdateMessage updMsg = (UpdateMessage) tRqstMsg;
            txWriter.queueMessage(updMsg.messageToBytes() );
            tAsset.update(updMsg.getValue());
            pTxCntx.addTimestamp("start to send response");
            pTxCntx.sendResponse(createUpdateResponse(tAssetId, tRqstMsg.getRequestId(), true, "Asset id (" + tAssetId + ") updated to: " + updMsg.getValue()));
            pTxCntx.addTimestamp("response sent");
            return;
        }
        pTxCntx.addTimestamp("start to send response");
        pTxCntx.sendResponse(createUpdateResponse(tAssetId, tRqstMsg.getRequestId(), true, "Unknown request message type: " + tRqstMsg.getClass().getName()));
        pTxCntx.addTimestamp("response sent");
        mLogger.warn("Unknown request message type: " + tRqstMsg.getClass().getName());
    }
}
