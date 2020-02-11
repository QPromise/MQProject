package p2p.producer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import javax.swing.*;
import java.io.*;

/**
 * @ClassName StreamProducer
 * @Description 二进制文件传输
 * @Author Anhua
 * @Date 2019/9/14 21:25
 * @Version 1.0
 */
public class StreamProducer {
    // 默认的连接用户名
    private static final String USERNAME= "admin";
    // 默认的连接密码
    private static final String PASSWORD= "admin";
    // 默认的连接地址
    private static final String BROKE_URL="tcp://192.168.217.129:61616";
    //集群地址
    private static final String BROKEFAILOVER_URL="failover:(tcp://192.168.217.138:61616,tcp://192.168.217.133:61616)?randomize=false";
    //连接工厂
    private static ConnectionFactory connectionFactor = null;
    //连接
    private static Connection connection = null;
    //会话
    private static Session session = null;
    //目的地
    private static Destination destination = null;
    //生产者
    private static MessageProducer messageProducer = null;
    //消息
    private static StreamMessage streamMessage = null;

    public static void main(String[] args) {

        // 选择文件
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("请选择要传送的文件");
        if (fileChooser.showOpenDialog(null) != JFileChooser.APPROVE_OPTION) {
            return;
        }
        File file = fileChooser.getSelectedFile();

        connectionFactor = new ActiveMQConnectionFactory(USERNAME, PASSWORD, BROKEFAILOVER_URL);
        try {
            connection = connectionFactor.createConnection();
            connection.start();

            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue("stream_queue");
            messageProducer = session.createProducer(destination);

            //设置优先级
            messageProducer.setPriority(9);
            //设置持久化模式
            messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

            streamMessage = session.createStreamMessage();

            streamMessage.setStringProperty("FILE_NAME",file.getName());
            streamMessage.setStringProperty("FILE_SIZE",String.valueOf(file.length()));
            InputStream in = new FileInputStream(file);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024*128*2*2*2];
            int c = -1;
            while((c = in.read(buffer)) > 0){
                bos.write(buffer);
                //System.out.println("send: "  + c);
            }
            streamMessage.writeBytes(bos.toByteArray());
            messageProducer.send(streamMessage);
            System.out.println("发送完成！");
            bos.close();
            in.close();
            messageProducer.close();
            session.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
