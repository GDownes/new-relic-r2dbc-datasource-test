package io.instil;

import com.newrelic.api.agent.Trace;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.InetSocketAddress;

public class DatastoreTest {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting server on port 8005");
        HttpServer server = HttpServer.create(new InetSocketAddress(8005), 0);
        server.createContext("/", new MyHandler());
        server.setExecutor(null);
        server.start();
    }

    static class MyHandler implements HttpHandler {
        @Override
        @Trace(metricName = "handle", dispatcher = true)
        public void handle(HttpExchange request) throws IOException {
            switch(request.getRequestURI().toString()) {
                case "/postgres": {
                    ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbc:postgresql://username:password@localhost:5432/gerarddownes");
                    Connection connection = Mono.from(connectionFactory.create()).block();
                    Mono.from(connection.createStatement("CREATE TABLE IF NOT EXISTS USERS(id int primary key, first_name varchar(255), last_name varchar(255), age int);").execute()).block();
                    Mono.from(connection.createStatement("TRUNCATE TABLE USERS;").execute()).block();
                    basicRequests(connection);
                    request.sendResponseHeaders(200, 0);
                }
                case "/mssql": {
                    ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbc:mssql://SA:Strong.Pwd-123@localhost:1433/gerarddownes");
                    Connection connection = Mono.from(connectionFactory.create()).block();
                    Mono.from(connection.createStatement("CREATE TABLE IF NOT EXISTS USERS(id int primary key, first_name varchar(255), last_name varchar(255), age int);").execute()).block();
                    Mono.from(connection.createStatement("TRUNCATE TABLE USERS;").execute()).block();
                    basicRequests(connection);
                    request.sendResponseHeaders(200, 0);
                }
                default: {
                    request.sendResponseHeaders(404, 0);
                }
            }
        }
    }


    @Trace(dispatcher = true)
    public static void basicRequests(Connection connection) {
        Mono.from(connection.createStatement("INSERT INTO USERS(id, first_name, last_name, age) VALUES(1, 'Max', 'Power', 30)").execute()).block();
        Mono.from(connection.createStatement("SELECT * FROM USERS WHERE last_name='Power'").execute()).block();
        Mono.from(connection.createStatement("UPDATE USERS SET age = 36 WHERE last_name = 'Power'").execute()).block();
        Mono.from(connection.createStatement("SELECT * FROM USERS WHERE last_name='Power'").execute()).block();
        Mono.from(connection.createStatement("DELETE FROM USERS WHERE last_name = 'Power'").execute()).block();
        Mono.from(connection.createStatement("SELECT * FROM USERS").execute()).block();
    }
}
