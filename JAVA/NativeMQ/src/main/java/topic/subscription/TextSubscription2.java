package topic.subscription;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @ClassName TextSubscription1
 * @Description TODO
 * @Author Anhua
 * @Date 2019/9/14 22:17
 * @Version 1.0
 */
public class TextSubscription2 {
    // 默认的连接用户名
    private static final String USERNAME= "admin";
    // 默认的连接密码
    private static final String PASSWORD= "admin";
    // 默认的连接地址
    private static final String BROKE_URL="tcp://192.168.217.129:61616";
    //集群地址
    private static final String BROKE_FAILOVER_URL="failover:(tcp://192.168.217.132:61616,tcp://192.168.217.133:61616)?randomize=false";
    //连接工厂
    private static ConnectionFactory connectionFactor = null;
    //连接
    private static Connection connection = null;
    //会话
    private static Session session = null;
    //目的地
    private static Destination destination = null;
    //生产者
    private static MessageConsumer messageConsumer = null;
    //消息
    private static TextMessage textMessage = null;

    public static void main(String[] args) {
        connectionFactor = new ActiveMQConnectionFactory(USERNAME, PASSWORD, BROKE_FAILOVER_URL);
        try {
            connection = connectionFactor.createConnection();
            connection.start();

            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            destination = session.createTopic("text_topic");
            messageConsumer = session.createConsumer(destination);

            messageConsumer.setMessageListener(
                    new MessageListener() {
                        public void onMessage(Message message) {
                            try {
                                TextMessage textMessage = (TextMessage) message;
                                System.out.println("收到的消息：" + textMessage.getText());
                                System.out.println("ID:" + textMessage.getJMSMessageID());
                                System.out.println("原始ID" + textMessage.getStringProperty("originalMsgId"));
                                System.out.println(System.currentTimeMillis()-textMessage.getJMSTimestamp());
                            } catch (JMSException e) {
                                e.printStackTrace();
                            }
                        }
                    }
            );
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
