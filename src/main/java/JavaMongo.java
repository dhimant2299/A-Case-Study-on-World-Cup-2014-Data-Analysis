import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class JavaMongo {

	public static void main(String[] args) {
	    // Creating a MongoDB client for connection
	    MongoClient mongoClient = new MongoClient("localhost", 27017);
	    System.out.println("Created successfully");

	    // Connect to your specific database
	    MongoDatabase database = mongoClient.getDatabase("WorldCup2014");

	    // Get the collection
	    MongoCollection<Document> collection = database.getCollection("Country");
	    MongoCollection<Document> match_Results = database.getCollection("Match_results");
	    MongoCollection<Document> players = database.getCollection("Players");
	    MongoCollection<Document> player_Cards = database.getCollection("Player_Cards");
	    MongoCollection<Document> player_Goals = database.getCollection("Player_Assists_goals");
	    
		  //****************************************************************
	    // first query to retrieve the list of country names that have won a World Cup
	    List<String> countries = getCountriesThatWonWorldCup(collection);

	    // Define the statement for the first query
	    String statement = "1. The list of country names that have won a World Cup:"
	    		+ "" ;
	    // Print the statement and the countries to the console
	    System.out.println(statement);
	    for (String country : countries) {
	        System.out.println(country);
	    }
	    // Save the statement and countries to a text file
	    saveToTextFile(countries, "output.txt", statement);

		  //****************************************************************
	    // Run your second query to retrieve the list of countries and trophies
	    List<String> countriesAndTrophies = getCountriesAndTrophies(collection);
	    // Define the statement for the second query
	    String statement2 = "2. The list of country names and the number of World Cups won (descending order):";
	    // Print the statement and the countries/trophies to the console
	    System.out.println(statement2);
	    for (String countryAndTrophies : countriesAndTrophies) {
	        System.out.println(countryAndTrophies);
	    }
	    // Save the statement and countries/trophies to a text file
	    saveToTextFile(countriesAndTrophies, "output.txt", statement2);
	        
	  //****************************************************************
	    // Run the third query to retrieve the capitals of countries with a population greater than 100 million
	    List<String> capitals = getCapitalByPopulation(collection);

	    // Define the statement for the third query
	    String statement3 = "3. Capitals of countries with a population greater than 100 million (in increasing order):";
	    // Print the statement and the capitals to the console
	    System.out.println(statement3);
	    for (String capital : capitals) {
	        System.out.println(capital);	    }
	    // Save the statement and capitals to a text file
	    saveToTextFile(capitals, "output.txt", statement3);	    
	 // Run the fourth query to retrieve the stadiums that hosted matches with more than 4 goals by a single team
	    List<String> stadiums = getStadiumsByGoals(match_Results, 4);
	    
	  //****************************************************************
	    // Define the statement for the fourth query
	    String statement4 = "4. Stadiums that hosted matches with more than 4 goals by a single team:";
	    // Print the statement and the stadiums to the console
	    System.out.println(statement4);
	    for (String stadium : stadiums) {
	        System.out.println(stadium); }
	    // Save the statement and stadiums to a text file
	    saveToTextFile(stadiums, "output.txt", statement4);
	    
	  //****************************************************************
	 // Run the fifth query to retrieve cities with stadiums starting with "Estadio"
	    List<String> citiesWithEstadio = getCitiesWithEstadioStadium(match_Results);
	    // Define the statement for the fifth query
	    String statement5 = "5. Cities with stadiums starting with 'Estadio':";
	    // Print the statement and the cities to the console
	    System.out.println(statement5);
	    for (String city : citiesWithEstadio) {
	        System.out.println(city);	    }
	    // Save the statement and cities to a text file
	    saveToTextFile(citiesWithEstadio, "output.txt", statement5);
	    
	  //****************************************************************
	 // Run the sixth query to retrieve stadiums and the number of matches hosted by each stadium
	    Map<String, Integer> stadiumsAndMatches = getStadiumsAndMatches(match_Results);
	    // Define the statement for the sixth query
	    String statement6 = "6. Stadiums and the number of matches hosted by each stadium:";
	    // Print the statement and the stadiums/matches to the console
	    System.out.println(statement6);
	    for (Map.Entry<String, Integer> entry : stadiumsAndMatches.entrySet()) {
	        System.out.println(entry.getKey() + " - Matches Hosted: " + entry.getValue());
	    }
	    // Save the statement and stadiums/matches to a text file
	    saveToTextFileFromMap(stadiumsAndMatches, "output.txt", statement6);
	    
	  //****************************************************************
	 // Run the seventh query to retrieve players with heights greater than 198 cms
	    List<String> tallPlayers = getPlayersByHeight(players, 198);

	    // Define the statement for the seventh query
	    String statement7 = "7. Players with heights greater than 198 cms:";

	    // Print the statement and the players to the console
	    System.out.println(statement7);
	    for (String player : tallPlayers) {
	        System.out.println(player);
	    }

	    // Save the statement and players to a text file
	    saveToTextFile(tallPlayers, "output.txt", statement7);
	    
	  //****************************************************************
	  //Run the eighth query
	    List<String> matchesInRange = getMatchesInRange(match_Results, "'2014-06-20'", "'2014-06-24'");
	 // the statement for the eighth query
	 String statement8 = "8. Matches played between 20-Jun-2014 and 24-Jun-2014:";
	 // Print the statement and the matches to the file
	 System.out.println(statement8);
	 for (String match : matchesInRange) {
	     System.out.println(match);}
	 // Save the statement and matches to a text file
	 saveToTextFile(matchesInRange, "output.txt", statement8);
	 
	 // RUn the ninth query *******************************************
	 List<String> captainInfoList = getCaptainInfoWithCardsAndGoals(players, player_Cards, player_Goals);
	// Define the statement for the ninth query
	String statement9 = "9. Captain Info with more than 2 Yellow cards or 1 Red card and their goals:";
	// Print the statement and the captain info to the file
	System.out.println(statement9);
	for (String captainInfo : captainInfoList) {
	    System.out.println(captainInfo);}
	// Save the info to a text file
	saveToTextFile(captainInfoList, "output.txt", statement9);
	//****************************************************************
	    // Close the MongoDB client
	    mongoClient.close();
	}

   private static List<String> getCountriesThatWonWorldCup(MongoCollection<Document> collection) {
        List<String> countries = new ArrayList<>();

        // Query to retrieve documents where the "trophy" field is greater than 0
        Document query = new Document("trophy", new Document("$gt", 0));

        // Projection to include only the "C_name" field
        Document projection = new Document("C_name", 1).append("_id", 0);

        // Find documents matching the query and apply the projection
        MongoCursor<Document> cursor = collection.find(query).projection(projection).iterator();
        try {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                String countryName = document.getString("C_name");
                countries.add(countryName);
            }
        } finally {
            cursor.close();
        }

        return countries;
    }
   
   private static List<String> getCountriesAndTrophies(MongoCollection<Document> collection) {
       List<String> countriesAndTrophies = new ArrayList<>();

       // Projection to include "C_name" and "trophy" fields
       Document projection = new Document("C_name", 1).append("trophy", 1).append("_id", 0);

       // Find documents where the "trophy" field is greater than 0
       MongoCursor<Document> cursor = collection.find(new Document("trophy", new Document("$gt", 0)))
                                               .projection(projection)
                                               .sort(new Document("trophy", -1))
                                               .iterator();

       try {
           while (cursor.hasNext()) {
               Document document = cursor.next();
               String countryName = document.getString("C_name");
               int trophies = document.getInteger("trophy");
               countriesAndTrophies.add(countryName + " - Trophies: " + trophies);
           }
       } finally {
           cursor.close();
       }

       return countriesAndTrophies;
   }

   private static List<String> getCapitalByPopulation(MongoCollection<Document> collection) {
	    List<String> capitalList = new ArrayList<>();

	    // Query to retrieve documents where the "Population" field is greater than 100 million
	    Document query = new Document("Population", new Document("$gt", 100));

	    // Projection to include both "C_name" and "Capital" fields
	    Document projection = new Document("C_name", 1).append("Capital", 1).append("_id", 0);

	    // Sort documents by "Population" in ascending order
	    Document sort = new Document("Population", 1);

	    // Find documents matching the query, apply the projection, and sort the results
	    MongoCursor<Document> cursor = collection.find(query).projection(projection).sort(sort).iterator();

	    try {
	        while (cursor.hasNext()) {
	            Document document = cursor.next();
	            String countryName = document.getString("C_name");
	            String capital = document.getString("Capital");
	            capitalList.add(countryName + " - Capital: " + capital);
	        }
	    } finally {
	        cursor.close();
	    }

	    return capitalList;
	}

   private static List<String> getStadiumsByGoals(MongoCollection<Document> collection, int goalThreshold) {
	    List<String> stadiumList = new ArrayList<>();

	    // Query to retrieve documents where the "Team1_score" or "Team2_score" is greater than the specified threshold
	    Document query = new Document("$or",
	            Arrays.asList(
	                    new Document("Team1_score", new Document("$gt", goalThreshold)),
	                    new Document("Team2_score", new Document("$gt", goalThreshold))
	            )
	    );

	    // Projection to include only the "Stadium" field
	    Document projection = new Document("Stadium", 1).append("_id", 0);

	    // Find documents matching the query and apply the projection
	    MongoCursor<Document> cursor = collection.find(query).projection(projection).iterator();

	    try {
	        while (cursor.hasNext()) {
	            Document document = cursor.next();
	            String stadium = document.getString("Stadium");
	            stadiumList.add(stadium);
	        }
	    } finally {
	        cursor.close();
	    }

	    return stadiumList;
	}

   private static List<String> getCitiesWithEstadioStadium(MongoCollection<Document> collection) {
	    Set<String> uniqueCities = new HashSet<>();
	    List<String> cities = new ArrayList<>();

	    // Query to retrieve documents where the stadium name starts with "Estadio"
	    Document query = new Document("Stadium", new Document("$regex", "^'Estadio"));

	    // Projection to include only the "Host_city" field
	    Document projection = new Document("Host_city", 1).append("_id", 0);

	    // Find documents matching the query and apply the projection
	    MongoCursor<Document> cursor = collection.find(query).projection(projection).iterator();
	    try {
	        while (cursor.hasNext()) {
	            Document document = cursor.next();
	            String city = document.getString("Host_city");

	            // Check if the city is unique before adding it to the list
	            if (uniqueCities.add(city)) {
	                cities.add(city);
	            }
	        }
	    } finally {
	        cursor.close();
	    }

	    return cities;
	}

   private static Map<String, Integer> getStadiumsAndMatches(MongoCollection<Document> collection) {
	    Map<String, Integer> stadiumsAndMatches = new HashMap<>();

	    // Group by stadium and count the number of matches for each stadium
	    Document groupStage = new Document("$group",
	            new Document("_id", "$Stadium")
	                    .append("matches", new Document("$sum", 1)));

	    // Project the output to rename the "_id" field to "Stadium"
	    Document projectStage = new Document("$project",
	            new Document("Stadium", "$_id")
	                    .append("matches", 1)
	                    .append("_id", 0));

	    // Sort the result by the number of matches in descending order
	    Document sortStage = new Document("$sort", new Document("matches", -1));

	    // Create a pipeline with the stages
	    List<Document> pipeline = Arrays.asList(groupStage, projectStage, sortStage);

	    // Execute the aggregation pipeline
	    AggregateIterable<Document> result = collection.aggregate(pipeline);

	    // Iterate over the result and populate the map
	    for (Document document : result) {
	        String stadium = document.getString("Stadium");
	        int matches = document.getInteger("matches");
	        stadiumsAndMatches.put(stadium, matches);
	    }

	    return stadiumsAndMatches;
	}

   private static List<String> getPlayersByHeight(MongoCollection<Document> collection, int minHeight) {
	    List<String> players = new ArrayList<>();

	    // Query to retrieve documents where the "Height" field is greater than the specified threshold
	    Document query = new Document("Height", new Document("$gt", minHeight));

	    // Projection to include "Fname", "Lname", and "DOB" fields
	    Document projection = new Document("Fname", 1).append("Lname", 1).append("DOB", 1).append("_id", 0);

	    // Find documents matching the query and apply the projection
	    MongoCursor<Document> cursor = collection.find(query).projection(projection).iterator();

	    try {
	        while (cursor.hasNext()) {
	            Document document = cursor.next();
	            String firstName = document.getString("Fname");
	            String lastName = document.getString("Lname");
	            String dob = document.getString("DOB");
	            players.add(firstName + " " + lastName + " - Date of Birth: " + dob);
	        }
	    } finally {
	        cursor.close();
	    }

	    return players;
	}

   private static List<String> getMatchesInRange(MongoCollection<Document> collection, String startDate, String endDate) {
	    List<String> matchesInRange = new ArrayList<>();

	    // Define the query to retrieve documents within the date range
	    Document query = new Document("Date", new Document("$gte", startDate).append("$lte", endDate));

	    // Projection to include relevant fields
	    Document projection = new Document("Stadium", 1).append("Team1", 1).append("Team2", 1).append("Date", 1).append("_id", 0);

	    // Find documents matching the query and apply the projection
	    MongoCursor<Document> cursor = collection.find(query).projection(projection).iterator();

	    try {
	        while (cursor.hasNext()) {
	            Document document = cursor.next();
	            String stadium = document.getString("Stadium");
	            String team1 = document.getString("Team1");
	            String team2 = document.getString("Team2");
	            String date = document.getString("Date");

	            String matchInfo = String.format("Stadium: %s, Team1: %s, Team2: %s, Date: %s", stadium, team1, team2, date);
	            matchesInRange.add(matchInfo);
	        }
	    } finally {
	        cursor.close();
	    }

	    return matchesInRange;
	}

   private static List<String> getCaptainInfoWithCardsAndGoals(MongoCollection<Document> playersCollection,
           MongoCollection<Document> cardsCollection,
           MongoCollection<Document> assistsGoalsCollection) {
List<String> captainInfoList = new ArrayList<>();

// Join Players, Player_Cards, and Player_Assists_goals collections based on player_ID
List<Document> joinedData = joinCollections(playersCollection, cardsCollection, assistsGoalsCollection, "player_ID");

// Filter the results based on the specified conditions
for (Document document : joinedData) {
boolean isCaptain = document.getBoolean("Is_Captain", false);
int yellowCards = document.getInteger("No_of_Yellow_cards", 0);
int redCards = document.getInteger("No_of_Red_cards", 0);

if (isCaptain && (yellowCards > 2 || redCards > 0)) {
String fName = document.getString("Fname");
String lName = document.getString("Lname");
String position = document.getString("Position");
int goals = document.getInteger("Goals", 0);

String captainInfo = String.format("Fname: %s, Lname: %s, Position: %s, Goals: %d", fName, lName, position, goals);
captainInfoList.add(captainInfo);
}
}

return captainInfoList;
}

private static List<Document> joinCollections(MongoCollection<Document> collection1,
MongoCollection<Document> collection2,
MongoCollection<Document> collection3,
String joinField) {
// Define the pipeline for the aggregation
List<Document> pipeline = Arrays.asList(
new Document("$lookup",
new Document("from", collection2.getNamespace().getCollectionName())
.append("localField", joinField)
.append("foreignField", joinField)
.append("as", "cards")),
new Document("$unwind", "$cards"),
new Document("$lookup",
new Document("from", collection3.getNamespace().getCollectionName())
.append("localField", joinField)
.append("foreignField", joinField)
.append("as", "assists_goals")),
new Document("$unwind", "$assists_goals"),
new Document("$project",
new Document("player_ID", 1)
.append("Fname", 1)
.append("Lname", 1)
.append("Position", 1)
.append("Is_Captain", 1)
.append("No_of_Yellow_cards", "$cards.No_of_Yellow_cards")
.append("No_of_Red_cards", "$cards.No_of_Red_cards")
.append("Goals", "$assists_goals.Goals"))
);

// Execute the aggregation pipeline
AggregateIterable<Document> result = collection1.aggregate(pipeline);

// Convert the result to a list
List<Document> resultList = new ArrayList<>();
result.into(resultList);

return resultList;
}
   
   private static void saveToTextFileFromMap(Map<String, Integer> data, String fileName, String statement) {
	    // Save the data to a text file
	    try (FileWriter writer = new FileWriter(new File(fileName), true)) {
	        if (statement != null) {
	            // Write the statement to the file
	            writer.write(statement + System.lineSeparator());
	        }

	        // Write the data to the file
	        for (Map.Entry<String, Integer> entry : data.entrySet()) {
	            writer.write(entry.getKey() + " - Matches Hosted: " + entry.getValue() + System.lineSeparator());
	        }

	        System.out.println("Data saved to " + fileName);
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}

   private static void saveToTextFile(List<String> citiesWithEstadio, String fileName, String statement) {
	    // Save the data to a text file
	    try (FileWriter writer = new FileWriter(new File(fileName), true)) {
	        if (statement != null) {
	            // Write the statement to the file
	            writer.write(statement + System.lineSeparator());
	        }

	        // Write the data to the file
	        for (String line : citiesWithEstadio) {
	            writer.write(line + System.lineSeparator());
	        }

	        System.out.println("Data saved to " + fileName);
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	   
	}
}

    

   

