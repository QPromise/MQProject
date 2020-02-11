package p2p.consumer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.*;

/**
 * @ClassName StreamConsumer
 * @Description 接收bytes类型数据
 * @Author Anhua
 * @Date 2019/9/14 21:27
 * @Version 1.0
 */
public class StreamConsumer {
    // 默认的连接用户名
    private static final String USERNAME= "admin";
    // 默认的连接密码
    private static final String PASSWORD= "admin";
    // 默认的连接地址
    private static final String BROKE_URL="tcp://192.168.217.129:61616";
    //集群地址
    private static final String BROKE_FAILOVER_URL="failover:(tcp://192.168.217.138:61616,tcp://192.168.217.133:61616)?randomize=false";
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
    private static StreamMessage streamMessage = null;

    public static void main(String[] args) {
        connectionFactor = new ActiveMQConnectionFactory(USERNAME, PASSWORD, BROKE_FAILOVER_URL);
        try {
            connection = connectionFactor.createConnection();
            connection.start();

            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue("stream_queue");
            messageConsumer = session.createConsumer(destination);

            messageConsumer.setMessageListener(
                    new MessageListener() {
                        @Override
                        public void onMessage(Message message) {
                            try {
                                streamMessage = (StreamMessage) message;
                                byte[] bytes = new byte[Integer.valueOf(streamMessage.getStringProperty("FILE_SIZE"))];
                                streamMessage.readBytes(bytes);
                                File file = new File("E:\\"+streamMessage.getStringProperty("FILE_NAME"));
                                FileOutputStream fileOutputStream = null;
                                if(!file.exists()){
                                    file.createNewFile();//如果文件不存在，就创建该文件
                                    fileOutputStream = new FileOutputStream(file);//首次写入获取
                                }else{
                                    //如果文件已存在，那么就在文件末尾追加写入
                                    fileOutputStream = new FileOutputStream(file,true);//这里构造方法多了一个参数true,表示在文件末尾追加写入
                                }
                                fileOutputStream.write(bytes);
                                fileOutputStream.flush();
                                fileOutputStream.close();

                                System.out.println("ID:" + streamMessage.getJMSMessageID());
                                System.out.println("原始ID" + streamMessage.getStringProperty("originalMsgId"));
                                System.out.println(System.currentTimeMillis()-streamMessage.getJMSTimestamp());
                            } catch (Exception e) {
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
