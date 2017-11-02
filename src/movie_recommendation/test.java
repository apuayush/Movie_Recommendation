//import java.net.UnknownHostException;
import java.util.Date;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class test{
    public static void main(String args[])
    {
        try{
            MongoClient mongo = new MongoClient("localhost",27017);
            DB db = mongo.getDB("testdb");

            /**
             * ** Get collection / table from 'testdb' ***
             */
            // if collection doesn't exists, MongoDB will create it for you
            DBCollection table = db.getCollection("user");

            /**
             * ** Insert ***
             */
            // create a document to store key and value
            BasicDBObject document = new BasicDBObject();
            document.put("name", "mkyong");
            document.put("age", 30);
            document.put("createdDate", new Date());
            table.insert(document);
            System.out.println(document);
            
            /**** Find and display ****/
	BasicDBObject searchQuery = new BasicDBObject();
	searchQuery.put("name", "mkyong");

	DBCursor cursor = table.find(searchQuery);

	while (cursor.hasNext()) {
		System.out.println(cursor.next());
	}

	/**** Update ****/
            
       
       
    } catch (MongoException e) {
	e.printStackTrace();
    }
    }
}