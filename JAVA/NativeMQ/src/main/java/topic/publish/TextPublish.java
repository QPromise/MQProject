package topic.publish;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @ClassName TextSubscription1
 * @Description TODO
 * @Author Anhua
 * @Date 2019/9/14 22:16
 * @Version 1.0
 */
public class TextPublish {
    // 默认的连接用户名
    private static final String USERNAME= "admin";
    // 默认的连接密码
    private static final String PASSWORD= "admin";
    // 默认的连接地址
    private static final String BROKE_URL="tcp://192.168.217.129:61616";
    //集群地址
    //tcp://localhost:61616?jms.blobTransferPolicy.defaultUploadUrl=http://localhost:8161/fileserver/
    private static final String BROKEFAILOVER_URL="failover:(tcp://192.168.217.132:61616,tcp://192.168.217.133:61616)?randomize=false";
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
    private static TextMessage textMessage = null;

    public static void main(String[] args) {

        connectionFactor = new ActiveMQConnectionFactory(USERNAME, PASSWORD, BROKEFAILOVER_URL);
        try {
            connection = connectionFactor.createConnection();
            connection.start();

            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            destination = session.createTopic("text_topic");
            messageProducer = session.createProducer(destination);

            //设置优先级
            messageProducer.setPriority(9);
            //设置持久化模式
            messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

            textMessage = session.createTextMessage();
            textMessage.setText("我是消息内容");
            messageProducer.send(textMessage);

            messageProducer.close();
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
