package TeamProjHBase.TeamProjHBase;

import Q1.Q1Servlet;
import Q2.HbaseQuery2DAO;
import Q2.Q2Servlet;
import Q3.HbaseQuery3DAO;
import Q3.Q3Servlet;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;

import javax.servlet.ServletException;

import static io.undertow.servlet.Servlets.defaultContainer;
import static io.undertow.servlet.Servlets.deployment;
import static io.undertow.servlet.Servlets.servlet;

public class Main {

    public static final String PATH = "/";

    public static void main(String[] args) throws Exception {
        try {
            new HbaseQuery2DAO();
            new HbaseQuery3DAO();
            DeploymentInfo servletBuilder = deployment()
                    .setClassLoader(Main.class.getClassLoader())
                    .setContextPath(PATH)
                    .setDeploymentName("handler.war")
                    .addServlets(
                            servlet("Q1Servlet", Q1Servlet.class)
                                    .addMapping("/q1"),
                            servlet("Q2Servlet", Q2Servlet.class)
                                    .addMapping("/q2"),
                            servlet("Q3Servlet", Q3Servlet.class)
                                    .addMapping("/q3")
                    );

            DeploymentManager manager = defaultContainer().addDeployment(servletBuilder);
            manager.deploy();

            HttpHandler servletHandler = manager.start();
            PathHandler path = Handlers.path(Handlers.redirect(PATH))
                    .addPrefixPath(PATH, servletHandler);

            Undertow server = Undertow.builder()
                    .addHttpListener(80, "0.0.0.0")
                    .setHandler(path)
                    .build();
            server.start();
        } catch (ServletException e) {
            throw new RuntimeException(e);
        }
    }
}
