package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;


@SuppressWarnings("serial")
public class Packet implements Serializable {

	String type;
	String message;
	int version;
	String sender;
	String isReplica;
	HashMap<String, String> keyValue = new HashMap<String, String>();
	HashMap<String, String> keyValue2 = new HashMap<String, String>();
	HashMap<String, String> keyValue3 = new HashMap<String, String>();
	HashMap<String, Integer> keyVersion = new HashMap<String, Integer>();
	ArrayList<String[]> starKeys = new ArrayList<String[]>();
	ArrayList<Integer> versionList = new ArrayList<Integer>();
	ArrayList<String> deletes = new ArrayList<String>();
	ArrayList<String> deletes2 = new ArrayList<String>();
}
