import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.HashMap;

/**
 * The module for connection to the Mongo Database
 *
 * @author Daeshan B.
 * @version Jan 1, 2019
 */

public class MongoHandler {
    /**
     * This is an instance of {@link MongoDatabase} which handles the Database.
     */
    private static MongoDatabase database;

    /**
     * This is an instance of {@link MongoClient} which handles the connection to the {@link MongoDatabase}.
     */
    private static MongoClient client;

    /**
     * This is an instance of {@link MongoClient} which handles the connection to the {@link MongoDatabase}.
     */
    private static boolean isProduction = false;

    /**
     * This is a {@link MongoCollection} HashMap storing all the collections within the {@link MongoDatabase}.
     */

    public static HashMap<String, MongoCollection<Document>> collectionsMap = new HashMap<>();


    /**
     * Connect to {@link MongoDatabase} and then creates a {@link HashMap} of all collections.
     * system.indexes is not a collection we want to be able to read or write into from the getCollection so we will skip over it.
     *
     * @param databasename is the name of the database you want to connect to.
     */
    public static void connect(String ip, String databasename, String username, String password) {
        try {
            String uri;

            if (isProduction) {
                uri = "mongodb://" + username + ":" + password + ip + ":27017/?authSource=" + databasename;
            } else {
                password = PASSWORD;
                uri = "mongodb://daeshan:" + password + "@localhost";
            }


            client = MongoClients.create(uri);
            database = client.getDatabase(databasename);


            for (Document collections : database.listCollections()) {
                String name = collections.getString("name");
                if (name.equalsIgnoreCase("system.indexes")) continue;
                collectionsMap.put(name, database.getCollection(name));
                MessageManager.debug("Collection: " + name + " has been added.");
            }
        } catch (MongoException e) {
            MessageManager.debug("[MongoDB] was not able to create connection properly.");
        }

    }

    /**
     * Closes connection to {@link MongoDatabase} and then clear the Collection HashMap.
     */
    public static void closeConnection() {
        try {
            collectionsMap.clear();
            client.close();
        } catch (MongoException e) {
            MessageManager.debug("[MongoDB] was not able to close connection properly.");
        }
    }

    /**
     * This will return the value of a field in the {@link MongoDatabase}
     *
     * @param key        is the search field (i.e "uuid")
     * @param value      is the field value you are looking from from Key
     * @param collection collection you want tos search
     * @param field      is the field value you want
     * @return the requested field value
     */
    public static Object getFromDocument(String key, String value, String collection, String field) {
        Document document = getCollection(collection).find(new Document(key, value)).first();
        return document.get(field);
    }

    /**
     * This will set the value of a field in the {@link MongoDatabase}
     *
     * @param key        is the search field (i.e "uuid")
     * @param value      is the field value you are looking from from Key
     * @param collection collection you want to search
     * @param field      is the field value you want to set
     * @param fieldvalue is the value of field you are setting.
     */
    public static void setInDocument(String key, String value, String collection, String field, Object fieldvalue) {
        Document document = getCollection(collection).find(new Document(key, value)).first();
        if (document == null) return;
        Bson newValue = new Document(field, fieldvalue);
        Bson updateOperationDocument = new Document("$set", newValue);
        getCollection(collection).updateOne(document, updateOperationDocument);
    }

    /**
     * @param collection collection you want to search for.
     * @return {@link MongoCollection}
     */

    public static MongoCollection<Document> getCollection(String collection) {
        MongoCollection<Document> mongoCollection = collectionsMap.get(collection);
        if (mongoCollection != null) {
            return mongoCollection;
        }
        return null;
    }

    public static MongoClient getClient() {
        return client;
    }

    public static void setIsProduction(boolean isProduction) {
        MongoHandler.isProduction = isProduction;
    }
}
