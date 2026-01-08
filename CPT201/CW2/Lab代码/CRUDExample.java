///*
// * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
// * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
// */
//package xjtlu.cpt201.mongoDB;

/**
 *
 * Code taken from https://www.baeldung.com/java-mongodb
 * and revised by Wei Wang, CPT, SAT, XJTLU
 */
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

public class CRUDExample {

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

            MongoDatabase database = mongoClient.getDatabase("myMongoDb");

            boolean collectionExists = mongoClient.getDatabase("myMongoDb").listCollectionNames()
                    .into(new ArrayList<>()).contains("customers");
            if (!collectionExists) {
                database.createCollection("customers");
            }

            // print all collections in customers database
            System.out.println("myMongoDb contains the following collections.");
            database.listCollectionNames().forEach(System.out::println);

            // create data
            MongoCollection<Document> collection = database.getCollection("customers");
            Document document = new Document();
            document.put("name", "Leo Messi");
            document.put("company", "Barcelona FC");
            collection.insertOne(document);
            System.out.println("The record " + document.toString() + " is inserted to collection " + collection.toString());

            // update data
            Document query = new Document();
            query.put("name", "Leo Messi");
            Document newDocument = new Document();
            newDocument.put("company", "Maimi FC");
            Document updateObject = new Document();
            updateObject.put("$set", newDocument);
            collection.updateOne(query, updateObject);
            System.out.println("The record has been updated.");

            // read data
            Document searchQuery = new Document();
            searchQuery.put("name", "Leo Messi");
            FindIterable<Document> cursor = collection.find(searchQuery);
            try (final MongoCursor<Document> cursorIterator = cursor.cursor()) {
                while (cursorIterator.hasNext()) {
                    System.out.println(cursorIterator.next());
                }
            }
            System.out.println("The record has been retrieved.");

            // delete data
            Document deleteQuery = new Document();
            deleteQuery.put("name", "Leo Messi");
            collection.deleteOne(deleteQuery);
            System.out.println("The record has been deleted.");
        }
    }
}
