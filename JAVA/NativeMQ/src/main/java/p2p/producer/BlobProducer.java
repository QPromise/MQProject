package p2p.producer;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQBlobMessage;

import javax.jms.*;
import javax.swing.*;
import java.io.File;

/**
 * @ClassName BlobConsumer
 * @Description 大二进制文件生产者
 * @Author Liu Ketao
 * @Date 2019/9/14 21:25
 * @Version 1.0
 */
public class BlobProducer {
    public static void main(String[] args) {

        // 选择文件
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("请选择要传送的文件");
        if (fileChooser.showOpenDialog(null) != JFileChooser.APPROVE_OPTION) {
            return;
        }
        File file = fileChooser.getSelectedFile();

        // 获取 ConnectionFactory
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "tcp://192.168.217.138:61616?jms.blobTransferPolicy.defaultUploadUrl=ftp://anhua2015:0327@192.168.217.138/XXX");

        try {
            // 创建 Connection
            Connection connection = connectionFactory.createConnection();
            connection.start();
            // 创建 Session
            ActiveMQSession session = (ActiveMQSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // 创建 Destination
            Destination destination = session.createQueue("File.Transport");
            // 创建 Producer
            MessageProducer producer = session.createProducer(destination);
            // 设置持久性的话，文件也可以先缓存下来，接收端离线再连接也可以收到文件,非持久性为NON_PERSISTENT
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            // 构造 BlobMessage，用来传输文件
            ActiveMQBlobMessage blobMessage = (ActiveMQBlobMessage) session.createBlobMessage(file);
            blobMessage.setStringProperty("FILE.NAME", file.getName());
            blobMessage.setLongProperty("FILE.SIZE", file.length());
            blobMessage.setDeletedByBroker(true);
            System.out.println("开始发送文件：" + file.getName() + "，文件大小：" + file.length() + " 字节");
            // 发送文件
            producer.send(blobMessage);
            System.out.println("完成文件发送：" + file.getName());
            producer.close();
            session.close();
            // 不关闭 Connection, 程序则不退出
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
