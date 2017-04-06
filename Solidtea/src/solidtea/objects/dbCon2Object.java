package solidtea.objects;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Properties;

public class dbCon2Object {
    public String[] attributes;  //[name, serverIP, username]
    public Properties psProps;
    public HashMap<String, PreparedStatement> psHash;
    public Connection connection;

    public dbCon2Object(String[] attributes, Properties psProps, Connection connection) {
        this.attributes = attributes;
        this.psProps = psProps;
        this.connection = connection;
    }
}