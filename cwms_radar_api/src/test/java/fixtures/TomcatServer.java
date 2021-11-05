package fixtures;

import org.apache.catalina.startup.HostConfig;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.descriptor.web.SecurityConstraint;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.catalina.*;
import org.apache.catalina.connector.Connector;
import hthurow.tomcatjndi.*;

public class TomcatServer {
    private Tomcat tomcatInstance = null;
    private TomcatJNDI tomcatJNDI = null;

    public TomcatServer(final String baseDir, final String radarWar, final int port, final String contextName) throws Exception{

        setupContext();
        tomcatInstance = new Tomcat();
        tomcatInstance.setBaseDir(baseDir);
        tomcatInstance.getHost().setAppBase(baseDir);
        new File(tomcatInstance.getServer().getCatalinaBase(),"temp").mkdirs();
        tomcatInstance.setPort(port);
        Connector connector = tomcatInstance.getConnector();
        connector.setSecure(true);
        connector.setScheme("https");


        tomcatInstance.setSilent(false);
        tomcatInstance.enableNaming();
        tomcatInstance.getEngine();
        tomcatInstance.getHost().addLifecycleListener(new HostConfig());
        tomcatInstance.addContext("", null);
        tomcatInstance.getServer();


    }

    public void start() throws LifecycleException {
        tomcatInstance.start();
        System.out.println("Tomcat listening at http://localhost:" + tomcatInstance.getConnector().getPort());
    }

    public void await() {
        tomcatInstance.getServer().await();
    }

    public void stop() throws LifecycleException {
        tomcatInstance.stop();
        tomcatJNDI.tearDown();
    }


    private void setupContext() {
        String contextFileName = System.getProperty("TOMCAT_RESOURCES");
        System.out.println(contextFileName);
        String templateFileName = contextFileName +".template";
        tomcatJNDI = new TomcatJNDI();
        try{
            String contextXml = new String(Files.readAllBytes((Paths.get(templateFileName))));
            contextXml = contextXml.replace("RADAR_JDBC_URL",System.getProperty("RADAR_JDBC_URL"))
                                   .replace("RADAR_JDBC_USERNAME",System.getProperty("RADAR_JDBC_USERNAME"))
                                   .replace("RADAR_JDBC_PASSWORD",System.getProperty("RADAR_JDBC_PASSWORD"));
            Files.write(Paths.get(contextFileName),contextXml.getBytes());
            tomcatJNDI.processContextXml(new File(contextFileName));
            tomcatJNDI.start();
        } catch( Exception err ){
            throw new RuntimeException("Failed to created full context.xml", err);
        }
    }



    public static void main(String args[]) {
        String baseDir = args[0];
        String radarWar = args[1];
        String contextName = args[2];
        int port = Integer.parseInt(System.getProperty("RADAR_LISTEN_PORT","0").trim());

        try {
            TomcatServer tomcat = new TomcatServer(baseDir, radarWar, port, contextName);
            tomcat.start();
            tomcat.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }
}
