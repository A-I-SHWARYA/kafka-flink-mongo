package TestElements;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.nest.bluehydrogen.kafkaFlinkMongo.sink.MongoDbSink;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.Assert.assertEquals;

@SpringBootTest
public class MongoDbSinkTest {
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;
    private String uri = "mongodb://localhost:27017";
    private String database = "databasenew";
    private String collectionName = "databasenewcollection";

    @Before
    public void setUp() throws Exception {
        mongoClient = MongoClients.create(uri);
        collection = mongoClient.getDatabase(database).getCollection(collectionName);
    }

    @After
    public void tearDown() throws Exception {
        collection.deleteMany(new Document());

        mongoClient.close();
    }

    @Test
    public void testInvoke() throws Exception {
        MongoDbSink sink = new MongoDbSink(uri, database, collectionName);
        sink.open(null);
        Document document = new Document("name", "John Doe");
        sink.invoke(document, null);
        assertEquals(1, collection.countDocuments());
        sink.close();
    }
}
