package movie_recommendation;

//package com.mongodb.mahout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Date;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class Movie_Recommendation{
    
	public static void main(String[] args) {
		try {
			
			long time1 = new Date().getTime();
			BufferedReader br = new BufferedReader( new FileReader( new File( "/home/apurvnit/Projects/java_projects/Movie_Recommendation/data2/movies.csv" ) ));			
			MongoClient mongo = new MongoClient("localhost",27017);
			DB db = mongo.getDB("movieLens");
			DBCollection collection = db.getCollection("movies");
			String s = br.readLine();
                        s=br.readLine();
			int i=0;
			while( s != null ) {
                                
                            try{
                                    int movie_id = Integer.parseInt(s.substring(0,s.indexOf(',')));

                                    String name = s.substring(s.indexOf(',')+1,s.lastIndexOf(','));
                                    String title = name.substring(0, name.indexOf("(")-1);
                                    String yearOfRel;
                                yearOfRel = name.substring(name.indexOf("(") + 1, name.indexOf(")")).replaceAll("\\s+","");
                                    String[] genres = s.substring(s.lastIndexOf(',')).split("\\|");

                                    BasicDBObject object = new BasicDBObject();
                                    object.put("_id", movie_id);
                                    object.put("title", title);
                                    object.put("year_of_release", yearOfRel);

                                    BasicDBList list = new BasicDBList();
                                    for( String genre: genres ) {
                                            list.add( genre );
                                    }

                                    object.put("genres", genres);



                                    collection.insert( object );
                                  
                                    
                        }
                            catch(Exception e){
                                continue;
                            }
                            i++;
                            System.out.println(i);
			s = br.readLine();
			}

			br.close();
			System.out.println("Inserted " +i+ " Movies");
//
			br = new BufferedReader( new FileReader( new File( "/home/apurvnit/Projects/java_projects/Movie_Recommendation/data2/tags.csv" ) ));			
			collection = db.getCollection("tags");
			s = br.readLine();
                        s = br.readLine();
			i=0;
			while( s != null ) {
                            try{
                                String[] sArr = s.split(",");
                                int user_id = Integer.parseInt( sArr[0] );
                                int movie_id = Integer.parseInt( sArr[1] );
                                String tag = sArr[2];
                                long timeStamp = Long.parseLong( sArr[3] );
                                BasicDBObject object = new BasicDBObject();
                                object.put("user_id", user_id);
				object.put("item_id", movie_id);
                                object.put("tag", tag);
                                object.put("timestamp", timeStamp);

                                collection.insert( object );
                                i++;
                                System.out.println(i + " " + sArr[3]);
                                s = br.readLine();
                            }
                            catch(Exception e){
                            continue;
                        }
				
				
					
			}	
			
			br.close();
			System.out.println("Inserted " +i+ " Tags");
			
			br = new BufferedReader( new FileReader( new File( "/home/apurvnit/Projects/java_projects/Movie_Recommendation/data/ratings.csv" ) ));			
			collection = db.getCollection("ratings");
			br.readLine();
                        s = br.readLine();
			i=0;
			while( s != null ) {
                            try{
				String[] sArr = s.split(",");
				int user_id = Integer.parseInt( sArr[0] );
				int movie_id = Integer.parseInt( sArr[1] );
				String rating = sArr[2];
				long timeStamp = Long.parseLong( sArr[3] );

				BasicDBObject object = new BasicDBObject();
				object.put("user_id", user_id);
				object.put("item_id", movie_id);
				object.put("preference", rating);
				object.put("timestamp", timeStamp);
				
				collection.insert( object );
				i++;
				s = br.readLine();				
                            }
                            catch(Exception e){
                                continue;
                            }
                         }
			
			br.close();
			System.out.println("Inserted " +i+ " Ratings");

			mongo.close();
			
			long time2 = new Date().getTime();
			long timeTaken = (time2-time1)/1000;
			System.out.println("Completed in " +(int)timeTaken+ " Seconds");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}

