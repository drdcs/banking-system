package com.distributed;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Mock database that contains a map from a user to its country of residence.
 *
 */

public class UserResidenceDatabase {

    private static final String USER_RESIDENCE_DETAILS = "user-residence.txt";
    private final Map<String, String> userResidenceMap;


    public UserResidenceDatabase() {
        this.userResidenceMap = loadUserResidenceFromFile();
    }

    public String getUserResidency(String user) {
        if(!userResidenceMap.containsKey(user)) {
            throw new RuntimeException("user " + user + " doesn't exist!");
        }
        return userResidenceMap.get(user);
    }

    private Map<String, String> loadUserResidenceFromFile() {
        Map<String, String> userResidenceMap = new HashMap<>();

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(USER_RESIDENCE_DETAILS);
        Scanner scanner = new Scanner(inputStream);

        while(scanner.hasNext()) {
            String line = scanner.nextLine();
            String [] userResidencePair = line.split(" ");
            userResidenceMap.put(userResidencePair[0], userResidencePair[1]);
        }

        return Collections.unmodifiableMap(userResidenceMap);
    }
}
