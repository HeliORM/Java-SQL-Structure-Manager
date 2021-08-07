package me.legrange.sql;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;

/**  SQL connection that ignores the 'close' method. Useful for constructing a connection pool
 * from one connection.
 */
public final class UnclosableConnection  {

    public static Connection getConnection(Connection connection) {
            return (Connection) Proxy.newProxyInstance(
                    UnclosableConnection.class.getClassLoader(),
                    new Class[]{Connection.class},
                    new ConnectionHandler(connection));
        }


    private static class ConnectionHandler implements InvocationHandler {

        private Connection con;

        private ConnectionHandler(Connection con) {
            this.con = con;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "close":
                    return null;
                default:
                    return method.invoke(con, args);
            }
        }
    }

    UnclosableConnection() {
    }
}