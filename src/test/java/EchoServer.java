import com.hoddmimes.txtest.aux.net.TcpServer;
import com.hoddmimes.txtest.aux.net.TcpServerCallbackIf;
import com.hoddmimes.txtest.aux.net.TcpThread;
import com.hoddmimes.txtest.aux.net.TcpThreadCallbackIf;
import com.hoddmimes.txtest.generated.ipc.messages.EchoMessage;
import com.hoddmimes.txtest.generated.ipc.messages.IPCFactory;

import java.io.IOException;

public class EchoServer implements TcpServerCallbackIf, TcpThreadCallbackIf
{
    private final int mPort = 7878;
    private IPCFactory mFactory;


    public static void main(String[] args) {
        EchoServer echoServer = new EchoServer();
        echoServer.declareAndRun();
    }

    private void declareAndRun() {
        mFactory = new IPCFactory();
        TcpServer mTcpServer = new TcpServer( this );
        try {
            mTcpServer.declareServer(mPort );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void tcpInboundConnection(TcpThread pThread)
    {
        pThread.setCallback( this );
        pThread.start();
        System.out.println("Inbound connect: " + pThread.toString());
    }

    @Override
    public void tcpMessageRead(TcpThread pThread, byte[] pBuffer) {
        EchoMessage msg = (EchoMessage) mFactory.createMessage(pBuffer);
        try {pThread.send( pBuffer );}
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void tcpErrorEvent(TcpThread pThread, IOException pException) {
        System.out.println("Disconnnect: " + pThread.toString() + " reason: " + pException.getMessage());
    }
}
