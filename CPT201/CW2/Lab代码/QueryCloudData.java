import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import java.util.ArrayList;
import org.bson.Document;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class QueryCloudData {
    public static void main(String[] args) {
        String connectionString = "mongodb+srv://QiCao:20040915Cq@cluster0.oo74ur3.mongodb.net/?appName=Cluster0";
        ServerApi serverApi = ServerApi.builder()
                .version(ServerApiVersion.V1)
                .build();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .serverApi(serverApi)
                .build();
        try (MongoClient mongoClient = MongoClients.create(settings)) {

            MongoDatabase database = mongoClient.getDatabase("sample_airbnb");


            // print all collections in customers database
            System.out.println("myMongoDb contains the following collections.");
            database.listCollectionNames().forEach(System.out::println);


            MongoCollection<Document> collection = database.getCollection("listingsAndReviews");


            // query data
            Document searchQuery = new Document();
            searchQuery.put("name", "Private Room in Bushwick");
            FindIterable<Document> cursor = collection.find(searchQuery);
            try (final MongoCursor<Document> cursorIterator = cursor.cursor()) {
                while (cursorIterator.hasNext()) {
                    System.out.println(cursorIterator.next());
                }
            }
            System.out.println("The record has been retrieved.");

        }
    }
}