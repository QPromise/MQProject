package p2p.consumer;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.BlobMessage;
import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import javax.jms.*;
import javax.swing.*;
import java.io.*;

/**
 * @ClassName BlobConsumer
 * @Description TODO
 * @Author Anhua
 * @Date 2019/9/14 21:27
 * @Version 1.0
 */
public class BlobConsumer {
    //ftp服务器地址
    public static String FTP_HOST_NAME = "192.168.217.138";
    //ftp服务器端口号默认为21
    public static Integer FTP_PORT = 21 ;
    //ftp登录账号
    public static String FTP_USER_NAME = "anhua2015";
    //ftp登录密码
    public static String FTP_PASSWORD = "0327";

    public static FTPClient ftpClient = null;

    public static void main(String[] args) {
        // 获取 ConnectionFactory
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(tcp://192.168.217.138:61616" +
                ",tcp://192.168.217.138:61616)?randomize=false");
        try {
            // 创建 Connection
            Connection connection = connectionFactory.createConnection();
            connection.start();
            // 创建 Session
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            // 创建 Destination
            Destination destination = session.createQueue("File.Transport");
            // 创建 Consumer
            MessageConsumer consumer = session.createConsumer(destination);
            // 注册消息监听器，当消息到达时被触发并处理消息
            consumer.setMessageListener(new MessageListener() {
                // 监听器中处理消息
                @Override
                public void onMessage(Message message) {
                    if (message instanceof BlobMessage) {
                        ActiveMQBlobMessage blobMessage = (ActiveMQBlobMessage) message;
                        try {
                            String fileName = blobMessage.getStringProperty("FILE.NAME");
                            System.out.println("文件接收请求处理：" + fileName + "，文件大小：" + blobMessage.getLongProperty("FILE.SIZE")
                                    + " 字节");

                            JFileChooser fileChooser = new JFileChooser();
                            fileChooser.setDialogTitle("请指定文件保存位置");
                            fileChooser.setSelectedFile(new File(fileName));

                            if (fileChooser.showSaveDialog(null) == JFileChooser.APPROVE_OPTION) {
                                File file = fileChooser.getSelectedFile();
                                OutputStream os = new FileOutputStream(file);
                                System.out.println("开始接收文件：" + fileName);
                                InputStream inputStream = blobMessage.getInputStream();
                                // 写文件，你也可以使用其他方式
                                byte[] buff = new byte[256];
                                int len = 0;
                                while ((len = inputStream.read(buff)) > 0) {
                                    os.write(buff, 0, len);
                                }

                                //删除ftp服务器文件
                                //blobMessage.deleteFile();
                                ftpOption(blobMessage);
                                blobMessage.acknowledge();

                                os.close();
                                System.out.println("完成文件接收：" + fileName);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /**
     * 删除ftp服务器上的文件
     */
    public static void ftpOption(ActiveMQBlobMessage blobMessage) {
        ftpClient = new FTPClient();
        ftpClient.setControlEncoding("utf-8");
        try {
            System.out.println("connecting...ftp服务器:"+FTP_HOST_NAME+":"+FTP_PORT);
            //连接ftp服务器
            ftpClient.connect(FTP_HOST_NAME, FTP_PORT);
            //登录ftp服务器
            ftpClient.login(FTP_USER_NAME, FTP_PASSWORD);
            //是否成功登录服务器
            int replyCode = ftpClient.getReplyCode();
            if(!FTPReply.isPositiveCompletion(replyCode)){
                System.out.println("connect failed...ftp服务器:"+FTP_HOST_NAME+":"+FTP_PORT);
            }
            System.out.println("connect successfu...ftp服务器:"+FTP_USER_NAME+":"+FTP_PORT);
            //切换FTP目录
            ftpClient.changeWorkingDirectory("opt/anhua2015");
            String[] str = blobMessage.getURL().toString().split("/");
            ftpClient.dele(str[str.length-1]);
            ftpClient.logout();

            if(ftpClient.isConnected()){
                ftpClient.disconnect();
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
