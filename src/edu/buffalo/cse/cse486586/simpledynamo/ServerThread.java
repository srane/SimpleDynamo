package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import android.app.IntentService;
import android.content.ContentResolver;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;

public class ServerThread extends IntentService {

	public static final String TAG = ServerThread.class.getName();
	Uri mUri;
	ContentResolver cr;

	public ServerThread() {
		super("ServerThread");
		// TODO Auto-generated constructor stub
	}

	@Override
	protected synchronized void onHandleIntent(Intent intent) {

		Socket clientSocket;
		ServerSocket serverSocket = SimpleDynamoProvider.serverSocket;

		/* Send Request for Recovering Lost Data */
		Packet getReplica = new Packet();
		getReplica.type = "GETREPLICA";
		getReplica.sender = SimpleDynamoProvider.myPort;
		sendPacket(
				getReplica,
				SimpleDynamoProvider.namePort
						.get(SimpleDynamoProvider.membership[(SimpleDynamoProvider.myID + 1) % 5]));

		Packet getOriginal = new Packet();
		getOriginal.type = "GETORIGINAL";
		getOriginal.sender = SimpleDynamoProvider.myPort;
		sendPacket(
				getOriginal,
				SimpleDynamoProvider.namePort
						.get(SimpleDynamoProvider.membership[(SimpleDynamoProvider.myID + 3) % 5]));

		while (true) {
			try {
				clientSocket = serverSocket.accept();
				ObjectInputStream ois = new ObjectInputStream(
						clientSocket.getInputStream());
				Object obj = ois.readObject();
				Packet receivedPacket = (Packet) obj;

				/* Received Request for Recovery Data and Sending Recovery Data */
				if (receivedPacket.type.equalsIgnoreCase("GETREPLICA")) {

					Packet replyReplica = new Packet();
					replyReplica.type = "REPLYREPLICA";

					replyReplica.keyValue = SimpleDynamoProvider.replica;
					replyReplica.keyValue2 = SimpleDynamoProvider.originalReplica;

					replyReplica.deletes = SimpleDynamoProvider.deleteOriginalReplica;
					SimpleDynamoProvider.deleteOriginalReplica.clear();
					receivedPacket.deletes2 = SimpleDynamoProvider.deleteReplica;
					SimpleDynamoProvider.deleteReplica.clear();

					replyReplica.keyVersion = SimpleDynamoProvider.keyVersion;

					sendPacket(replyReplica, receivedPacket.sender);

					// Log.e(TAG,
					// "Sending YOUR original and previous avds replica values to: "
					// + receivedPacket.sender);
				}

				else if (receivedPacket.type.equalsIgnoreCase("GETORIGINAL")) {

					Packet replyOriginal = new Packet();
					replyOriginal.type = "REPLYORIGINAL";
					replyOriginal.keyValue = SimpleDynamoProvider.original;
					replyOriginal.keyVersion = SimpleDynamoProvider.keyVersion;

					replyOriginal.deletes = SimpleDynamoProvider.deleteOriginal;
					SimpleDynamoProvider.deleteOriginal.clear();
					sendPacket(replyOriginal, receivedPacket.sender);

					// Log.e(TAG,
					// "Sending originals OF previous avd values to: "
					// + receivedPacket.sender);

				}

				/* Received Recovery Data and now Processing and Storing it */
				else if (receivedPacket.type.equalsIgnoreCase("REPLYREPLICA")) {

					String[] deleteOriginals = receivedPacket.deletes
							.toArray(new String[receivedPacket.deletes.size()]);

					for (int i = 0; i < deleteOriginals.length; i++) {
						deleteFile(deleteOriginals[i]);
						// Log.e(TAG, "DELETE ORIGINALS: " +
						// deleteOriginals[i]);
						SimpleDynamoProvider.keyVersion
								.remove(deleteOriginals[i]);
					}

					SimpleDynamoProvider.deleteOriginal = receivedPacket.deletes;

					String[] deleteOriginalReplicas = receivedPacket.deletes2
							.toArray(new String[receivedPacket.deletes2.size()]);

					for (int i = 0; i < deleteOriginalReplicas.length; i++) {
						deleteFile(deleteOriginalReplicas[i]);
						// Log.e(TAG, "DELETE ORIGINALREPLICAS: "
						// + deleteOriginalReplicas[i]);
						SimpleDynamoProvider.keyVersion
								.remove(deleteOriginalReplicas[i]);
					}

					SimpleDynamoProvider.deleteOriginalReplica = receivedPacket.deletes2;

					synchronized (SimpleDynamoProvider.iteratorLock) {
						Iterator it = receivedPacket.keyValue2.entrySet()
								.iterator();
						while (it.hasNext()) {
							Map.Entry entry = (Entry) it.next();

							String filename = (String) entry.getKey();
							String value = (String) entry.getValue();

							if (SimpleDynamoProvider.keyVersion
									.containsKey(filename)) {

								if ((SimpleDynamoProvider.keyVersion
										.get(filename)) < (receivedPacket.keyVersion
										.get(filename))) {

									SimpleDynamoProvider.original.put(filename,
											value);

									SimpleDynamoProvider.keyVersion.put(
											filename, receivedPacket.keyVersion
													.get(filename));

									FileOutputStream fos = this.openFileOutput(
											filename, Context.MODE_PRIVATE);
									fos.write(value.getBytes());
									fos.close();
									// Log.e(TAG, "Original backup: KEY: "
									// + filename + "value: " + value);

								}
							} else {
								SimpleDynamoProvider.original.put(filename,
										value);

								SimpleDynamoProvider.keyVersion.put(filename,
										receivedPacket.keyVersion.get(entry
												.getKey()));

								FileOutputStream fos = this.openFileOutput(
										filename, Context.MODE_PRIVATE);
								fos.write(value.getBytes());
								fos.close();
								// Log.e(TAG, "Original backup: KEY: " +
								// filename
								// + "value: " + value);

							}
						}
					}
					synchronized (SimpleDynamoProvider.iteratorLock) {
						Iterator it2 = receivedPacket.keyValue.entrySet()
								.iterator();
						while (it2.hasNext()) {
							Map.Entry entry2 = (Entry) it2.next();

							String filename2 = (String) entry2.getKey();
							String value2 = (String) entry2.getValue();

							if (SimpleDynamoProvider.keyVersion
									.containsKey(filename2)) {

								if ((SimpleDynamoProvider.keyVersion
										.get(filename2)) < (receivedPacket.keyVersion
										.get(filename2))) {

									SimpleDynamoProvider.originalReplica.put(
											filename2, value2);
									SimpleDynamoProvider.keyVersion.put(
											filename2,
											receivedPacket.keyVersion
													.get(filename2));

									FileOutputStream fos = this.openFileOutput(
											filename2, Context.MODE_PRIVATE);
									fos.write(value2.getBytes());
									fos.close();
									// Log.e(TAG,
									// "one previous avd replica backup: KEY: "
									// + filename2 + "value: "
									// + value2);
								}
							} else {
								SimpleDynamoProvider.originalReplica.put(
										filename2, value2);
								SimpleDynamoProvider.keyVersion.put(filename2,
										receivedPacket.keyVersion
												.get(filename2));

								FileOutputStream fos = this.openFileOutput(
										filename2, Context.MODE_PRIVATE);
								fos.write(value2.getBytes());
								fos.close();
								// Log.e(TAG,
								// "one previous avd replica backup: KEY: "
								// + filename2 + "value: "
								// + value2);

							}
						}
					}

					// Log.e(TAG,
					// "Recevied MY originals and previous avds values");
				}

				else if (receivedPacket.type.equalsIgnoreCase("REPLYORIGINAL")) {
					String[] deleteReplicas = receivedPacket.deletes
							.toArray(new String[receivedPacket.deletes.size()]);

					for (int i = 0; i < deleteReplicas.length; i++) {
						deleteFile(deleteReplicas[i]);
						// Log.e(TAG, "DELETE Replicas: " + deleteReplicas[i]);
						SimpleDynamoProvider.keyVersion
								.remove(deleteReplicas[i]);
					}

					SimpleDynamoProvider.deleteReplica = receivedPacket.deletes;
					synchronized (SimpleDynamoProvider.iteratorLock) {

						Iterator it = receivedPacket.keyValue.entrySet()
								.iterator();
						while (it.hasNext()) {
							Map.Entry entry = (Entry) it.next();

							String filename = (String) entry.getKey();
							String value = (String) entry.getValue();

							if (SimpleDynamoProvider.keyVersion
									.containsKey(filename)) {
								if ((SimpleDynamoProvider.keyVersion
										.get(filename)) < (receivedPacket.keyVersion
										.get(filename))) {

									SimpleDynamoProvider.replica.put(filename,
											value);
									SimpleDynamoProvider.keyVersion.put(
											filename, receivedPacket.keyVersion
													.get(filename));

									FileOutputStream fos = openFileOutput(
											filename, Context.MODE_PRIVATE);
									fos.write(value.getBytes());
									fos.close();
									// Log.e(TAG,
									// "two previous avd replica backup: KEY: "
									// + filename + "value: "
									// + value);
								}
							} else {
								SimpleDynamoProvider.replica.put(filename,
										value);
								SimpleDynamoProvider.keyVersion
										.put(filename,
												receivedPacket.keyVersion
														.get(filename));

								FileOutputStream fos = openFileOutput(filename,
										Context.MODE_PRIVATE);
								fos.write(value.getBytes());
								fos.close();
								// Log.e(TAG,
								// "two previous avd replica backup: KEY: "
								// + filename + "value: " + value);

							}
						}
					}
					// Log.e(TAG,
					// "Received originals OF 2nd previous avd values");
				}

				else if (receivedPacket.type.equalsIgnoreCase("INSERTKEY")) {
					FileOutputStream outputStream;
					try {

						String filename = receivedPacket.sender;
						String value = receivedPacket.message;

						outputStream = this.openFileOutput(filename,
								Context.MODE_PRIVATE);
						outputStream.write(value.getBytes());
						outputStream.close();
						// Log.e(TAG, "Written to avd: " + filename + "Value: "
						// + value);

						if (SimpleDynamoProvider.keyVersion
								.containsKey(filename)) {
							int version = SimpleDynamoProvider.keyVersion
									.get(filename);

							SimpleDynamoProvider.keyVersion.put(filename,
									(version + 1));
							// Log.e(TAG, "Inserted version for key: " +
							// filename
							// + "Version: " + (version + 1));

						} else {
							SimpleDynamoProvider.keyVersion.put(filename, 1);
							// Log.e(TAG, "Inserted version for key: " +
							// filename
							// + "Version: " + 1);

						}

						if (receivedPacket.isReplica
								.equalsIgnoreCase("REPLICA")) {

							SimpleDynamoProvider.replica.put(filename, value);
							SimpleDynamoProvider.deleteReplica.remove(filename);
							// Log.e(TAG, "INSERTED for 2ND previous: " +
							// filename
							// + "Value: " + value);

						} else if (receivedPacket.isReplica
								.equalsIgnoreCase("ORIGINALREPLICA")) {

							SimpleDynamoProvider.originalReplica.put(filename,
									value);
							SimpleDynamoProvider.deleteOriginalReplica
									.remove(filename);
							// Log.e(TAG, "INSERTED for previous avd: " +
							// filename
							// + "Value: " + value);

						} else {

							// Log.e(TAG, "INSERTED original: " + filename
							// + "Value: " + value);

							SimpleDynamoProvider.original.put(filename, value);
							SimpleDynamoProvider.deleteOriginal
									.remove(filename);

						}

					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

				else if (receivedPacket.type.equalsIgnoreCase("QUERYKEY")) {

					String selection = receivedPacket.message;

					FileInputStream fis = openFileInput(selection);
					int bytes_left = fis.available();
					byte[] buffer = new byte[bytes_left];
					fis.read(buffer, 0, bytes_left);
					String value = new String(buffer, "UTF-8");
					fis.close();

					Packet queryReply = new Packet();
					queryReply.type = "QUERYREPLY";
					queryReply.sender = selection;
					queryReply.message = value;
					// NULL HERE??
					if (SimpleDynamoProvider.keyVersion.get(selection) == null) {

						queryReply.version = 1;
						if (queryReply.message == null) {
							queryReply.version = 0;
						}
						sendPacket(queryReply, receivedPacket.sender);

						// Log.e(TAG,
						// "NULL FOR: "
						// + selection
						// + " value: "
						// + value
						// + "version: "
						// + SimpleDynamoProvider.keyVersion
						// .get(selection));
					} else {
						queryReply.version = SimpleDynamoProvider.keyVersion
								.get(selection);
						sendPacket(queryReply, receivedPacket.sender);
					}
				}

				else if (receivedPacket.type.equalsIgnoreCase("QUERYREPLY")) {
					if (SimpleDynamoProvider.latest_key
							.equalsIgnoreCase(receivedPacket.sender)) {

						SimpleDynamoProvider.responses++;
					}
					// Log.e(TAG, "QUERY RESPONSE: " + "Latest KEY: "
					// + SimpleDynamoProvider.latest_key + "KEY: "
					// + receivedPacket.sender + "value: "
					// + receivedPacket.message + "version: "
					// + receivedPacket.version);
					synchronized (SimpleDynamoProvider.lock) {
						if (SimpleDynamoProvider.latest_key
								.equalsIgnoreCase(receivedPacket.sender)) {
							if (receivedPacket.version > SimpleDynamoProvider.latest_version) {
								SimpleDynamoProvider.latest_value = receivedPacket.message;
								SimpleDynamoProvider.latest_version = receivedPacket.version;
							}

						} else {
							// Log.e(TAG, "Late Result");
						}
						if (SimpleDynamoProvider.responses == 2) {
							SimpleDynamoProvider.lock.notify();
						}
					}
				}

				else if (receivedPacket.type.equalsIgnoreCase("QUERYSTAR")) {
					String[] fileList = fileList();
					ArrayList<String[]> keyValue = new ArrayList<String[]>();
					ArrayList<Integer> version = new ArrayList<Integer>();
					for (int i = 0; i < fileList.length; i++) {
						String filename = fileList[i];
						FileInputStream fis;
						try {
							fis = openFileInput(filename);
							int bytes_left = fis.available();
							byte[] buffer = new byte[bytes_left];
							fis.read(buffer, 0, bytes_left);
							String value = new String(buffer, "UTF-8");
							fis.close();
							version.add(SimpleDynamoProvider.keyVersion
									.get(filename));
							keyValue.add(new String[] { filename, value });
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					Packet starResponse = new Packet();
					starResponse.type = "STARRESPONSE";
					starResponse.starKeys = keyValue;
					starResponse.versionList = version;
					sendPacket(starResponse, receivedPacket.sender);
				}

				else if (receivedPacket.type.equalsIgnoreCase("STARRESPONSE")) {
					SimpleDynamoProvider.starResponses++;
					synchronized (SimpleDynamoProvider.starLock) {

						ArrayList<String[]> temp = receivedPacket.starKeys;

						for (int i = 0; i < temp.size(); i++) {
							String[] keyNValue = temp.get(i);
							if (SimpleDynamoProvider.starVersion
									.containsKey(keyNValue[0])) {

								// if (SimpleDynamoProvider.starVersion
								// .get(keyNValue[0]) == null) {
								// SimpleDynamoProvider.starVersion.put(
								// keyNValue[0], 1);
								// }
								if (receivedPacket.versionList.get(i) == null) {
									receivedPacket.versionList.set(i, 1);
								}

								if (SimpleDynamoProvider.starVersion
										.get(keyNValue[0]) < receivedPacket.versionList
										.get(i)) {

									SimpleDynamoProvider.starMap.put(
											keyNValue[0], keyNValue[1]);

									SimpleDynamoProvider.starVersion.put(
											keyNValue[0],
											receivedPacket.versionList.get(i));
								}
							} else {

								SimpleDynamoProvider.starMap.put(keyNValue[0],
										keyNValue[1]);
								SimpleDynamoProvider.starVersion.put(
										keyNValue[0],
										receivedPacket.versionList.get(i));

							}
						}

						if (SimpleDynamoProvider.starResponses > 3) {
							SimpleDynamoProvider.starLock.notify();
						}
					}
				}

				else if (receivedPacket.type.equalsIgnoreCase("DELETEALL")) {
					String[] fileList = fileList();
					for (int i = 0; i < fileList.length; i++) {
						deleteFile(fileList[i]);
						SimpleDynamoProvider.original.remove(fileList[i]);
						SimpleDynamoProvider.originalReplica
								.remove(fileList[i]);
						SimpleDynamoProvider.replica.remove(fileList[i]);
						SimpleDynamoProvider.keyVersion.remove(fileList[i]);
					}

				}

				else if (receivedPacket.type.equalsIgnoreCase("DELETEENTRY")) {

					deleteFile(receivedPacket.message);

					if (receivedPacket.isReplica.equalsIgnoreCase("REPLICA")) {

						SimpleDynamoProvider.replica
								.remove(receivedPacket.message);

						SimpleDynamoProvider.deleteReplica
								.add(receivedPacket.message);
						// Log.e(TAG, "added to deleteReplica: "
						// + receivedPacket.message);

						SimpleDynamoProvider.keyVersion
								.remove(receivedPacket.message);
					}

					else if (receivedPacket.isReplica
							.equalsIgnoreCase("ORIGINALREPLICA")) {

						SimpleDynamoProvider.originalReplica
								.remove(receivedPacket.message);

						SimpleDynamoProvider.deleteOriginalReplica
								.add(receivedPacket.message);
						// Log.e(TAG, "added to deleteOriginalReplica: "
						// + receivedPacket.message);

						SimpleDynamoProvider.keyVersion
								.remove(receivedPacket.message);
					}

					else {

						SimpleDynamoProvider.original
								.remove(receivedPacket.message);

						SimpleDynamoProvider.deleteOriginal
								.add(receivedPacket.message);
						// Log.e(TAG, "added to deleteOriginal: "
						// + receivedPacket.message);

						SimpleDynamoProvider.keyVersion
								.remove(receivedPacket.message);

					}
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public synchronized void sendPacket(Packet p, String port) {

		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[] {
					10, 0, 2, 2 }), Integer.parseInt(port));
			OutputStream os = socket.getOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(os);
			synchronized (SimpleDynamoProvider.iteratorLock) {
				oos.writeObject(p);
			}
			oos.close();
			os.close();
			socket.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized void sendPacketToAll(Packet p) {

		String[] REMOTE_PORTS = { "11108", "11112", "11116", "11120", "11124" };

		for (int i = 0; i < REMOTE_PORTS.length; i++) {
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[] {
						10, 0, 2, 2 }), Integer.parseInt(REMOTE_PORTS[i]));
				OutputStream os = socket.getOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(os);
				synchronized (SimpleDynamoProvider.iteratorLock) {
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

}
