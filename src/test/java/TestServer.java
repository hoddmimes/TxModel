import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hoddmimes.txtest.aux.ipc.IpcController;
import com.hoddmimes.txtest.quorum.QuorumStateController;
import com.hoddmimes.txtest.server.ServerMessageSeqnoInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicLong;



public class TestServer extends Thread implements ServerMessageSeqnoInterface
{
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private AtomicLong mSeqno = new AtomicLong(0);
    private QuorumStateController mStateController;
    JsonObject jConfiguration;
    IpcController mIPCController;
    int mNodeId =  0;
    Logger mLogger = LogManager.getLogger(TestServer.class);

    public static void main(String[] args) {
        TestServer ts = new TestServer();
        ts.parseArguments( args );
        ts.loadConfiguration();
        ts.initialize();
    }


    private void parseArguments( String[] args ) {
        int i = 0;
        while( i < args.length) {
            if (args[i].equals("-id")) {
                mNodeId = Integer.parseInt(args[++i]);
            }
            if (args[i].equals("-seqno")) {
                mSeqno.set(Long.parseLong(args[++i]));
            }
            i++;
        }
        if (mNodeId == 0) {
            throw new RuntimeException("Server ID not specified as parameter");
        }
    }

    private void initialize() {
        mIPCController = new IpcController(mNodeId, jConfiguration);
        mStateController = new QuorumStateController( mNodeId, jConfiguration, mIPCController, this);
        mStateController.syncStateWithQuorumServer();
    }


    private void loadConfiguration() {
        try {
            jConfiguration = JsonParser.parseReader( new FileReader( "TxServer.json" ) ).getAsJsonObject();
        } catch( Exception e ) {
            mLogger.info( "ERROR: Unable to load configuration file");
            System.exit( 1 );
        }
    }


    @Override
    public long getServerMessageSeqno() {
        return mSeqno.get();
    }
}

