package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MatrixCursor.RowBuilder;
import android.net.Uri;
import android.telephony.TelephonyManager;

public class SimpleDynamoProvider extends ContentProvider {

	public static final String TAG = SimpleDynamoProvider.class.getName();
	Context context;
	static ServerSocket serverSocket;

	/* Storing RING DETAILS */
	static String membership[] = { "5562", "5556", "5554", "5558", "5560" };
	String nodeID[];
	static String myName;
	static String myPort;
	static HashMap<String, String> namePort;

	/* VERSIONS */
	static HashMap<String, Integer> keyVersion;
	static HashMap<String, Integer> starVersion;
	static HashMap<String, String> starMap;

	/* RECOVERY DATA */
	static HashMap<String, String> replica;
	static HashMap<String, String> originalReplica;
	static HashMap<String, String> original;

	/* Store pending Deletes */
	static ArrayList<String> deleteReplica;
	static ArrayList<String> deleteOriginal;
	static ArrayList<String> deleteOriginalReplica;

	static int latest_version;
	static int myID;
	static String latest_value;
	static String latest_key;
	static int responses;
	static int starResponses;

	/* Synchronization Primitives */
	static String lock = "lock";
	static String starLock = "starLock";
	static String recovery = "RECOVERY";
	static String iteratorLock = "IteratorLock";

	@Override
	public synchronized int delete(Uri uri, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		if (selection.equalsIgnoreCase("@")) {
			/* DELETING ALL STORED FILES */
			String[] fileList = context.fileList();
			for (int i = 0; i < fileList.length; i++) {
				context.deleteFile(fileList[i]);
			}

			/* CLEARING ALL RECOVERY DATA */
			replica.clear();
			original.clear();
			originalReplica.clear();

			/* CLEARING ALL DELETE RECOVERY DATA */
			deleteOriginal.clear();
			deleteReplica.clear();
			deleteOriginalReplica.clear();
			/* CLEARING ALL VERSION DATA */
			keyVersion.clear();

		} else if (selection.equalsIgnoreCase("*")) {
			Packet deleteAll = new Packet();
			deleteAll.type = "DELETEALL";
			sendPacketToAll(deleteAll);
		} else {

			int i = findPartition(selection);
			Packet deleteEntry = new Packet();
			deleteEntry.type = "DELETEENTRY";
			deleteEntry.message = selection;

			/* DELETE FROM OWN STORAGE AND FORWARD DELETE */
			if (membership[i].equalsIgnoreCase(myName)) {
				context.deleteFile(selection);
				keyVersion.remove(selection);
				original.remove(selection);
				deleteOriginal.add(selection);
				// Log.e(TAG, "Added to deleteOriginal: " + selection);
			} else {
				deleteEntry.isReplica = "ORIGINAL";
				sendPacket(deleteEntry, namePort.get(membership[i]));
			}

			deleteEntry.isReplica = "ORIGINALREPLICA";
			sendPacket(deleteEntry, namePort.get(membership[(i + 1) % 5]));
			deleteEntry.isReplica = "REPLICA";
			sendPacket(deleteEntry, namePort.get(membership[(i + 2) % 5]));
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String filename = values.getAsString("key");
		String value = values.getAsString("value");
		int i = findPartition(filename);

		Packet insertKey = new Packet();
		insertKey.type = "INSERTKEY";
		insertKey.message = value;
		insertKey.sender = filename;

		if (membership[i].equalsIgnoreCase(myName)) {

			FileOutputStream fos;
			synchronized (this) {
				try {
					fos = context
							.openFileOutput(filename, Context.MODE_PRIVATE);
					fos.write(value.getBytes());
					fos.close();

					if (keyVersion.containsKey(filename)) {
						int version = keyVersion.get(filename);
						keyVersion.put(filename, (version + 1));
					} else {
						keyVersion.put(filename, 1);
					}

					original.put(filename, value);
					deleteOriginal.remove(filename);
					// Log.e(TAG, "INSERTED original: " + filename + "Value: "
					// + value);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else {
			insertKey.isReplica = "ORIGINAL";
			sendPacket(insertKey, namePort.get(membership[i]));
		}

		insertKey.isReplica = "ORIGINALREPLICA";
		sendPacket(insertKey, namePort.get(membership[((i + 1) % 5)]));
		insertKey.isReplica = "REPLICA";
		sendPacket(insertKey, namePort.get(membership[((i + 2) % 5)]));

		return null;

	}

	@Override
	public synchronized boolean onCreate() {
		// TODO Auto-generated method stub
		context = getContext();

		replica = new HashMap<String, String>();
		original = new HashMap<String, String>();
		originalReplica = new HashMap<String, String>();
		keyVersion = new HashMap<String, Integer>();

		deleteOriginal = new ArrayList<String>();
		deleteReplica = new ArrayList<String>();
		deleteOriginalReplica = new ArrayList<String>();

		TelephonyManager tel = (TelephonyManager) context
				.getSystemService(Context.TELEPHONY_SERVICE);
		myName = tel.getLine1Number().substring(
				tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(myName) * 2));

		nodeID = new String[membership.length];
		for (int i = 0; i < membership.length; i++) {
			try {
				nodeID[i] = genHash(membership[i]);
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		namePort = new HashMap<String, String>();
		for (int i = 0; i < membership.length; i++) {
			namePort.put(membership[i],
					String.valueOf((Integer.parseInt(membership[i]) * 2)));
		}
		try {
			serverSocket = new ServerSocket(10000);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (int i = 0; i < membership.length; i++) {
			if (myName.equalsIgnoreCase(membership[i])) {
				myID = i;
			}
		}

		Intent serverIntent = new Intent(context, ServerThread.class);
		context.startService(serverIntent);

		return false;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection,
			String selection, String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		String[] columns = { "key", "value" };
		MatrixCursor mCursor = new MatrixCursor(columns);
		RowBuilder rb;
		latest_version = 0;
		latest_key = selection;
		latest_value = selection;

		if (selection.equalsIgnoreCase("*")) {

			starMap = new HashMap<String, String>();
			starVersion = new HashMap<String, Integer>();

			Packet queryStar = new Packet();
			queryStar.sender = myPort;
			queryStar.type = "QUERYSTAR";

			starResponses = 0;
			sendPacketToAll(queryStar);

			synchronized (starLock) {
				try {
					starLock.wait();
					starResponses = 0;
					synchronized (iteratorLock) {
						Iterator it = starMap.entrySet().iterator();

						while (it.hasNext()) {
							Map.Entry entry = (Entry) it.next();
							rb = mCursor.newRow();
							rb.add("key", entry.getKey());
							rb.add("value", entry.getValue());

						}
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		} else if (selection.equalsIgnoreCase("@")) {
			// Log.e(TAG, "I WAS QUERIED @");
			String[] fileList = context.fileList();
			for (int i = 0; i < fileList.length; i++) {
				String filename = fileList[i];
				FileInputStream fis;
				try {
					fis = context.openFileInput(filename);
					int bytes_left = fis.available();
					byte[] buffer = new byte[bytes_left];
					fis.read(buffer, 0, bytes_left);
					String value = new String(buffer, "UTF-8");
					fis.close();
					rb = mCursor.newRow();
					rb.add("key", filename);
					rb.add("value", value);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		} else {
			synchronized (lock) {

				int i = findPartition(selection);

				Packet queryKey = new Packet();
				queryKey.type = "QUERYKEY";
				queryKey.message = selection;
				queryKey.sender = SimpleDynamoProvider.myPort;

				responses = 0;
				sendPacket(queryKey, namePort.get(membership[i]));
				sendPacket(queryKey, namePort.get(membership[((i + 1) % 5)]));
				sendPacket(queryKey, namePort.get(membership[((i + 2) % 5)]));
				try {

					lock.wait();
					lock.wait(200);

					// Log.e(TAG, "QUERY:" + selection + "value: " +
					// latest_value);

					responses = 0;
					rb = mCursor.newRow();
					rb.add("key", selection);
					rb.add("value", latest_value);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		return mCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	public void sendPacket(Packet p, String port) {

		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[] {
					10, 0, 2, 2 }), Integer.parseInt(port));
			OutputStream os = socket.getOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(os);
			synchronized (iteratorLock) {
				oos.writeObject(p);
			}
			oos.close();
			os.close();
			socket.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void sendPacketToAll(Packet p) {

		String[] REMOTE_PORTS = { "11108", "11112", "11116", "11120", "11124" };

		for (int i = 0; i < REMOTE_PORTS.length; i++) {
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[] {
						10, 0, 2, 2 }), Integer.parseInt(REMOTE_PORTS[i]));
				OutputStream os = socket.getOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(os);
				synchronized (iteratorLock) {
					oos.writeObject(p);
				}
				oos.close();
				os.close();
				socket.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public int findPartition(String filename) {
		int partitionNode = -1;
		String hashedFname = null;
		try {
			hashedFname = genHash(filename);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < nodeID.length; i++) {
			if (i == 0) {
				if (hashedFname.compareToIgnoreCase(nodeID[i]) < 0) {
					partitionNode = i;
					break;
				} else if (hashedFname.compareToIgnoreCase(nodeID[4]) > 0) {
					partitionNode = i;
					break;
				}

			} else {
				if ((hashedFname.compareToIgnoreCase(nodeID[i]) < 0)
						&& (hashedFname.compareToIgnoreCase(nodeID[i - 1]) > 0)) {
					partitionNode = i;
					break;
				}
			}
		}
		return partitionNode;
	}

}
