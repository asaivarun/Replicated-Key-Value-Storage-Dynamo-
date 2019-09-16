package edu.buffalo.cse.cse486586.simpledynamo;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import static android.content.ContentValues.TAG;


public class SimpleDynamoProvider extends ContentProvider {
	static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
	static final String[] GEN_PORT = {"5554", "5556", "5558", "5560", "5562"};
	static final int SERVER_PORT = 10000;
	static String myPort;
	static String portStr;
	static String delimit = ";";
	static String delimit_value = "-";
	static String nodehash_predecessor = null;
	static String nodehash_sec_predecessor = null;
	static String nodehash_successor = null;
	static String nodehash_next_successor = null;
	static String second_successor_port,predecessor_port, second_predecessor_port;
	static String successor_port;
	static String failed_port=null;
	static String IamBack="justcame";
	static final String key = "key";
	static final String value = "value";
	static final String str = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";
	Uri uri = Uri.parse(str);
	static String node_id,my_node_id;
	static int my_index, index_successor, index_second_successor, index_predecessor, index_second_predecessor;
	static int nodecount=5;
//	static int time=8500;
	static int time1=7000;

	TreeMap<String, String> nodes = new TreeMap<String, String>();
	ArrayList<String> local_key_list = new ArrayList<String>();
	HashMap<String,String>predecessor_messages = new HashMap<String, String>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("*")){
			List<String> nodesport_list = new ArrayList<String>(nodes.values());
			for(String port:nodesport_list){
				if(!port.equals(myPort)){
					selection="@";
					try {
						Log.i(TAG,"S line 67 Sending delete @ to port"+port);
						Socket socket9 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port));
						PrintWriter outclient9 = new PrintWriter(socket9.getOutputStream(),
								true);
						outclient9.println("Delete"+delimit+selection);
						BufferedReader bufread9 = new BufferedReader(new InputStreamReader(socket9.getInputStream()));
						String rec9 = bufread9.readLine();
						if (rec9.equals("received")) {
							Log.i(TAG,"D: line 77; Deleted All values from pre "+ successor_port);
							socket9.close();
						}

					} catch (Exception e) {
						Log.e(TAG, "D : line 93  Exception can't forward message to nodehash_successor " + e.getMessage());
						e.printStackTrace();
					}

				}
			}
			selection="@";
		}
		if (selection.equals("@")) {
			Log.i(TAG,"In Delete @ ");
			String[] key_list = getContext().fileList();
			local_key_list.clear();
			for (String l:key_list) {
				local_key_list.add(l);
				Log.i(TAG,"D: line 66; deleting key : "+l);
				getContext().deleteFile(l);
				local_key_list.remove(l);
			}

		}


		else {
			String gen_key = null;
			try {
				gen_key = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			String key_port = keyhash_compare(gen_key);
			if (local_key_list.contains(selection) || key_port.equals(myPort)) {
				Log.i(TAG, " D : line 107 : Found query " + selection + "and deleted ");
				getContext().deleteFile(selection);
				local_key_list.remove(selection);
//					if(predecessor_messages.containsKey(selection)){
//						predecessor_messages.remove(selection);
//					}

				if (key_port.equals(myPort)) {
					String delete_single1="Delete_Replication"+delimit+selection;
					Log.i(TAG, "I : line 152;  Sending key to next suc node  port for deleting replication " + successor_port);
					try {
						Socket socket18 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(successor_port));
						PrintWriter outclient18 = new PrintWriter(socket18.getOutputStream(),
								true);
						outclient18.println(delete_single1);
						BufferedReader bufread18 = new BufferedReader(new InputStreamReader(socket18.getInputStream()));
						String rec18 = bufread18.readLine();
						if (rec18.equals("received")) {
							Log.i(TAG, "I : line 173; Received ack and closing socket with " + successor_port);
							socket18.close();
						}


					} catch (Exception e) {
						Log.e(TAG, "I : line 166  Exception can't forward message to nodehash_successor " + e.getMessage());
						e.printStackTrace();
					}


					Log.i(TAG, "I : line 152;  Sending key to next suc node  port for deleting replication " + second_successor_port);
					try {
						Socket socket17 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(second_successor_port));
						PrintWriter outclient17 = new PrintWriter(socket17.getOutputStream(),
								true);
						outclient17.println(delete_single1);
						BufferedReader bufread16 = new BufferedReader(new InputStreamReader(socket17.getInputStream()));
						String rec17 = bufread16.readLine();
						if (rec17.equals("received")) {
							Log.i(TAG, "I : line 173; Received ack and closing socket with " + second_successor_port);
							socket17.close();
						}


					} catch (Exception e) {
						Log.e(TAG, "I : line 166  Exception can't forward message to nodehash_successor " + e.getMessage());
						e.printStackTrace();


					}
				}
			}
			else if (!key_port.equals(myPort)){
				Log.i(TAG,"Q : line 356 : Sending delete  : " + selection +" to key_port  " + key_port);
				String delete_single="Delete"+delimit+selection;
				try {
					Socket socket15 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(key_port));
					PrintWriter outclient15 = new PrintWriter(socket15.getOutputStream(),
							true);

					outclient15.println(delete_single);
					BufferedReader bufread14 = new BufferedReader(new InputStreamReader(socket15.getInputStream()));
					String rec14 = bufread14.readLine();
					if (rec14.equals("received")) {
						socket15.close();
					}
				} catch (Exception e) {
					Log.e(TAG, "S : line 404  Exception can't send updated_prev  " + e.getMessage());
					e.printStackTrace();
				}
			}
		}

		return 1;
	}


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}


	public String get_max_version(String value1,String value2,String value3)
	{
		Log.i(TAG,"In line 277, finding max version with values 1 "+ value1+" 2 : "+value2 +" 3: "+ value3);
		String version="0";
		if((value2!=null && !value2.equals("NOT:FOUND")) && (value1!=null && !value1.equals("NOT:FOUND")) && (value3!=null && !value3.equals("NOT:FOUND"))) {
			version = Integer.toString( Math.max(Integer.parseInt(value2.split(delimit_value)[1]), Math.max(Integer.parseInt(value3.split(delimit_value)[1]), Integer.parseInt(value1.split(delimit_value)[1]))));
		}
		else if((value2!=null  && !value2.equals("NOT:FOUND")) && (value1!=null && !value1.equals("NOT:FOUND"))) {
			version = Integer.toString( Math.max(Integer.parseInt(value2.split(delimit_value)[1]),  Integer.parseInt(value1.split(delimit_value)[1])));
		}
		else if ((value1!=null && !value1.equals("NOT:FOUND")) && (value3!=null  && !value3.equals("NOT:FOUND"))) {
			version = Integer.toString( Math.max(Integer.parseInt(value3.split(delimit_value)[1]),  Integer.parseInt(value1.split(delimit_value)[1])));
		}
		else if ((value2!=null && !value2.equals("NOT:FOUND")) && (value3!=null  && !value3.equals("NOT:FOUND"))) {
			version = Integer.toString( Math.max(Integer.parseInt(value3.split(delimit_value)[1]),  Integer.parseInt(value2.split(delimit_value)[1])));
		}
		else if (value1!=null && !value1.equals("NOT:FOUND")){
			version=Integer.toString(Integer.parseInt(value1.split(delimit_value)[1]));
		}
		else if (value2!=null && !value2.equals("NOT:FOUND")){
			version=Integer.toString(Integer.parseInt(value2.split(delimit_value)[1]));
		}
		else if (value3!=null && !value3.equals("NOT:FOUND")){
			version=Integer.toString(Integer.parseInt(value3.split(delimit_value)[1]));
		}
		Log.i(TAG,"In line 241: found max version is : "+ version);
		return version;

	}
	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String default_version="1";
		String filename = values.getAsString("key");
		String fileContents = values.getAsString("value");
		Log.i(TAG, "I : line 257 ; In Insert : key "+ filename + " Values "+fileContents);
		String gen_key = null;
		try {
			gen_key = genHash(filename);
			Log.i(TAG," I : line 261 gen_hash for key : " +gen_key + " key :" + filename + " value : " + fileContents );
		} catch (NoSuchAlgorithmException e) {
			Log.i(TAG, " I: line 263 : Gen Hash failed for key "+ filename);
			e.printStackTrace();
		}
		String key_port=keyhash_compare(gen_key);
		String []key_successor=get_successor_next_successor_port(key_port);
		Log.i(TAG, " The key belongs to : "+ key_port);
			String updated_fileContents;
			String query_noversion = "Query_Single" + delimit + filename + delimit + "query_noversion";
			String[] myvalue = {null, null};
			String[] suc1 = {null, null};
			String[] suc2 = {null, null};
			if (key_port.equals(myPort)) {

				myvalue[0] = get_only_value(filename);
				Log.i(TAG, " Q: line 286 Found key " + filename + " value : " + myvalue[0]);
			} else {

				myvalue = get_query_socket(query_noversion, key_port);
				Log.i(TAG, " Q: line 279  Asking  key port " + filename + " value : " + myvalue[0]);
			}

			if (key_successor[0].equals(myPort)) {

				suc1[0] = get_only_value(filename);
				Log.i(TAG, " Q: line 286 I am  suc so found key " + filename + " value : " + suc1[0]);
			}
			else {


				suc1 = get_query_socket(query_noversion, key_successor[0]);
				Log.i(TAG, " Q: line 288  not suc so asking key " + filename + "and got value " + suc1[0] + " from suc " + key_successor[0]);


			}
			if (key_successor[1].equals(myPort)) {

					suc2[0] = get_only_value(filename);
					Log.i(TAG, " Q: line 295 I am 2nd suc so asking key " + filename + " value : " + suc2[0]);
			}
			else {

				suc2 = get_query_socket(query_noversion, key_successor[1]);
				Log.i(TAG, " Q: line 297  Not 2nd suc  asking key " + filename + "and got value "+suc2[0]+ "from suc " + key_successor[1]);
				}
				Log.i(TAG, " I: line 300, Key previous version key : " + filename + " value in key port is :" + myvalue[0] + " value in suc1 is " + suc1[0] + " value in suc2 is " + suc2[0]);
				String version = "1";
				if((myvalue[0]!=null && !myvalue[0].equals("NOT:FOUND")) || (suc1[0]!=null && !suc1[0].equals("NOT:FOUND")) && (suc2[0]!=null && !suc2[0].equals("NOT:FOUND"))) {

					version = Integer.toString(Integer.parseInt(get_max_version(myvalue[0], suc1[0], suc2[0])) + 1);
				}
				Log.i(TAG, " I: line 303, Key previous version key : " + filename + " value in key port is :" + myvalue + " value in suc1 is " + suc1[0] + " value in suc2 is " + suc2[0]);

				updated_fileContents = fileContents + delimit_value + version;
				Log.i(TAG, "I: line 306, Key is  " + filename + " updated value is : " + updated_fileContents);
				String message1 = "Replicate_Message" + delimit + gen_key + delimit + filename + delimit + updated_fileContents + delimit + myPort;
				if (key_port.equals(myPort)) {
					write_key_value(filename, updated_fileContents);
				}
				else{
					Log.i(TAG, "I : line 312;  Replication  " + message1 + " to suc node  port " + key_port);

					String failed_keyport=socket(message1,key_port);
					if (failed_keyport != null) {
						Log.i(TAG, "I : line 316; Replicaiton Key Node failed so  Sending key to " + key_port + "suc node " + key_successor[0]);
						String temp = socket("Failed_Replicate" + delimit + message1, key_successor[0]);

					}

				}
				if (key_successor[0].equals(myPort)) {
					write_key_value(filename, updated_fileContents);
				}
				else{
					Log.i(TAG, "I : line 327;  Replication  " + message1 + " to suc node  port " + key_successor[0]);

					String failed_keyport_suc=socket(message1,key_successor[0]);
					if (failed_keyport_suc != null) {
						Log.i(TAG, "I : line 331; Replicaiton Key Node failed so  Sending key to " + key_successor[0] + "suc node " + key_successor[0]);
						String temp = socket("Failed_Replicate" + delimit + message1, key_successor[1]);

					}

				}
				if (key_successor[1].equals(myPort)) {
					write_key_value(filename, updated_fileContents);
				}
				else{
					Log.i(TAG, "I : line 340;  Replication  " + message1 + " to suc node  port " + key_successor[1]);

					String failed_keyport_suc2=socket(message1,key_successor[1]);
					if (failed_keyport_suc2 != null) {
						String suc[] = get_successor_next_successor_port(key_successor[1]);
						Log.i(TAG, "I : line 345; Replicaiton Key Node failed so  Sending key to " + key_successor[1] + "suc node " + suc[0]);
						String temp1 = socket("Failed_Replicate" + delimit + message1, suc[0]);

					}

				}


			return uri;
		}

		public void write_key_value(String filename, String fileContent){
// fileContents.split(delimit)[0]
	 FileOutputStream outputStream;
		try {
			outputStream = getContext().openFileOutput(filename, Context.MODE_WORLD_WRITEABLE);
			outputStream.write(fileContent.getBytes());
			outputStream.close();
		} catch (Exception e) {
			Log.e(TAG, "I: line 143; File write failed");
		}
		Log.v(TAG,"I : line 145; inserted key : "+filename+ " value :" + fileContent);
		if(!local_key_list.contains(filename)){
			local_key_list.add(filename);
		}
	}

	public String keyhash_compare(String key_hash) {
		List<String> node_genkey_list = new ArrayList<String>(nodes.keySet());
		String current_port = null;
		for (String current_node_id :node_genkey_list) {
			int index=node_genkey_list.indexOf(current_node_id);
			String current_predecessor;
			if (index==0){
				current_predecessor=node_genkey_list.get(nodecount-1);
			}
			else{
				current_predecessor=node_genkey_list.get(index-1);
			}

			current_port=nodes.get(current_node_id);

//				Log.i(TAG, " C : line 186 :  keyid : " + key_hash + " node_id : " + node_id + " nodehash_predecessor: " + nodehash_predecessor + " pre port : " + second_successor_port + " succ port : " + successor_port + " succ : " + nodehash_successor);
//				if s1 > s2, it returns positive number
//				if s1 < s2, it returns negative number
//				if s1 == s2, it returns 0

			if (current_node_id.equals(current_predecessor)) {
				return current_port;
			}
			if ((current_predecessor.compareTo(key_hash) < 0 || current_node_id.compareTo(current_predecessor) < 0) && current_node_id.compareTo(key_hash) > 0) {
				return current_port;
			}
			if (current_node_id.compareTo(key_hash) < 0 && current_node_id.compareTo(current_predecessor) < 0 && current_predecessor.compareTo(key_hash) < 0) {
				return current_port;
			}

		}
		return current_port;
	}

	public String[] get_successor_next_successor_port(String current_port) {
		List<String> nodesport_list = new ArrayList<String>(nodes.values());


		int index=nodesport_list.indexOf(current_port);

		int send_successor_index = (index + 1)%(nodecount);
		int send_next_successor_index = (index + 2)%(nodecount);
		String []send_array_successor={nodesport_list.get(send_successor_index),nodesport_list.get(send_next_successor_index)};
		return send_array_successor;
	}

	public String socket(String message, String port){
		String failed=null;
		try{
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port));
			socket.setSoTimeout(time1);
			PrintWriter outclient = new PrintWriter(socket.getOutputStream(),
					true);
			outclient.println(message);
//			Thread.sleep(300);
			BufferedReader bufread = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String rec = bufread.readLine();
			Log.i(TAG, "S: line 374  Received reply from rec :"+rec + " from port "+port);
			if (rec.equals("received")) {
				Log.i(TAG, "I : line 374; Received ack and closing socket with " + port);
				socket.close();

			} else {
				failed = port;
				socket.close();
			}


		}catch (SocketTimeoutException e){
			Log.e(TAG,"C : line 416 SocketTimeout Exception "+e.getMessage() + message);
			e.printStackTrace();
			failed=port;

		}catch (NullPointerException e){
			Log.e(TAG,"C : line 337 Null pointer Exception "+e.getMessage());
			e.printStackTrace();
			failed=port;

		} catch (IOException e) {
			Log.e(TAG, "C : IOException: Client not sending final proposal "+e.getMessage()  );
			e.printStackTrace();
			failed=port;
		}catch (Exception e) {
			Log.e(TAG, "C : Exception: Client not sending final proposal "+e.getMessage() );
			e.printStackTrace();
			failed=port;
		}
		return failed;
	}

	public String[] get_query_socket(String message, String port){
		String []failed={null,null};
		try{
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port));
			socket.setSoTimeout(time1);
			PrintWriter outclient = new PrintWriter(socket.getOutputStream(),
					true);
			outclient.println(message);
//			Thread.sleep(300);
			BufferedReader bufread = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String rec = bufread.readLine();
			Log.i(TAG, "S: line 502 Received reply for msg "+message+ "from rec "+rec + " from port "+port);
			if (rec!=null) {
				Log.i(TAG, "I : line 505; Received value "+ rec+"and closing socket with " + port);
				socket.close();
				failed[0]=rec;

			} else {
				failed[1] = port;
				socket.close();
			}


		}catch (SocketTimeoutException e){
			Log.e(TAG,"C : line 416 SocketTimeout Exception "+e.getMessage() +message +  " port is " +port);
			e.printStackTrace();
			failed[1]=port;

		}catch (NullPointerException e){
			Log.e(TAG,"C : line 337 Null pointer Exception "+e.getMessage());
			e.printStackTrace();
			failed[1]=port;

		} catch (IOException e) {
			Log.e(TAG, "C : IOException: Client not sending final proposal "+e.getMessage()  );
			e.printStackTrace();
			failed[1]=port;
		}catch (Exception e) {
			Log.e(TAG, "C : Exception: Client not sending final proposal "+e.getMessage() );
			e.printStackTrace();
			failed[1]=port;
		}
		return failed;
	}



	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr)*2));
		second_successor_port =myPort;
		successor_port=myPort;
		try {

			for (int i=0;i<GEN_PORT.length;i++ ) {
				String port=GEN_PORT[i];
				String socket_port=REMOTE_PORT[i];
				node_id = genHash(port);
				if(port.equals(portStr)){
					my_node_id=node_id;
				}
				nodes.put(node_id, socket_port);
			}
			Log.i(TAG, " portStr : " + portStr + " myport :  " + myPort + " gen_node id : " + node_id);

		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, " O : line 184; genHash of myport failed");
			e.printStackTrace();
		}
		List<String> node_genkey_list = new ArrayList<String>(nodes.keySet());
		my_index=node_genkey_list.indexOf(my_node_id);

		index_successor = (my_index + 1)%(nodecount);
		index_second_successor = (my_index + 2)%(nodecount);
		if (my_index==0){
			index_predecessor =(nodecount-1);
			index_second_predecessor =(nodecount-2);
		}
		else{
			index_predecessor =my_index-1;
		}
		if(my_index==1){
			index_second_predecessor =(nodecount-1);
		}
		else if(my_index>1){
			index_second_predecessor =my_index-2;
		}

		nodehash_successor =node_genkey_list.get(index_successor);
		nodehash_next_successor =node_genkey_list.get(index_second_successor);
		nodehash_predecessor =node_genkey_list.get(index_predecessor);
		nodehash_sec_predecessor =node_genkey_list.get(index_second_predecessor);

		successor_port=nodes.get(nodehash_successor);
		second_successor_port =nodes.get(nodehash_next_successor);
		predecessor_port=nodes.get(nodehash_predecessor);
		second_predecessor_port =nodes.get(nodehash_sec_predecessor);



		Log.i(TAG," S: line 429; prev_prede port is" + second_predecessor_port + "prede port is" + predecessor_port + " my port :"+myPort+" suc port is : "+ successor_port+ " next suc port is : " + second_successor_port);

		try {

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {

			Log.e(TAG, " O : line :201; Can't create a ServerSocket");

		}

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,myPort);
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		// TODO Auto-generated method stub
		Log.i(TAG, " Q: line 520;  querying key   "+selection );
		MatrixCursor matrixcursor = new MatrixCursor(new String[]{"key", "value"});
		if(selection.equals("*")){
			List<String> nodesport_list = new ArrayList<String>(nodes.values());
			for(String port:nodesport_list){
				if(!port.equals(myPort)){
					try {
						Log.i(TAG, " Q: line 275;  Sending query @ to port " + port);

						Socket socket11 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port));
						PrintWriter outclient11 = new PrintWriter(socket11.getOutputStream(),
								true);
						String send_query="Query";
						outclient11.println(send_query);
						ObjectInputStream obj = new ObjectInputStream(socket11.getInputStream());
						Object object = null;
						HashMap<String, String> remote_pairs = new HashMap<String, String>();
						try {
							object = obj.readObject();
						} catch (Exception e) {
							Log.e(TAG, "Q : line 264; Object read failed " + e.getMessage());
							e.printStackTrace();
						}
						if (object != null) {
							Log.i(TAG," Received Map from suc "+ successor_port);
							remote_pairs = (HashMap<String, String>) object;
							for (Map.Entry eachpair : remote_pairs.entrySet()) {
								matrixcursor.newRow()
										.add("key", eachpair.getKey())
										.add("value", eachpair.getValue());
							}
							socket11.close();
						}
					} catch (Exception e) {
						Log.e(TAG, "Q : line 278;  Exception can't forward message for querying to nodehash_successor " + e.getMessage());
						e.printStackTrace();
					}
				}

			}

			selection="@";
			Log.i(TAG," S: line 283; Returning map fr querying * : "+ selection);


		}
		if(selection.equals("@")) {
			Log.i(TAG, "S : line 288; Returning all values from this local node ");
			String[] key_list = getContext().fileList();
//				local_key_list(key_list);
			local_key_list.clear();
			for (String i : key_list) {
				local_key_list.add(i);
				matrixcursor =get_value(i,matrixcursor);
			}
		}
		else {
			String gen_key = null;
			try {
				gen_key = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
//			try {
//				Thread.sleep(500);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			String failed = null;;
			String key_port;
			key_port = keyhash_compare(gen_key);
			String[] key_successor = get_successor_next_successor_port(key_port);
			Log.i(TAG, " Q: 566, keyport : " + key_port + " my port : " + myPort + " key in local list " + local_key_list.contains(selection));
			if(selectionArgs!=null && selectionArgs[0].equals("query_noversion")){
				if(local_key_list.contains(selection)) {
					matrixcursor = get_versioned_value_cursor(selection, matrixcursor);
				}
				else{
					matrixcursor.newRow()
							.add("key", selection)
							.add("value", "NOT:FOUND");
					Log.i(TAG,"Q : line 613;   query: "+ selection + " value " + "NOT:FOUND" + " and returned");
				}

			}
			else  {
//				if(local_key_list.contains(selection)){
				// TODO Auto-generated method stub
//				fileContents=fileContents+delimit+default_version;

				String query_noversion = "Query_Single" + delimit + selection + delimit + "query_noversion";
				String []myvalue={null,null};
				String []suc1={null,null};
				String []suc2={null,null};
				if(key_port.equals(myPort)){

					myvalue[0] = get_only_value(selection);
				}
				else {
					Log.i(TAG, " Q: line 984  Asking  key port " + selection + " value : "+suc1[0]) ;
					myvalue=get_query_socket(query_noversion,key_port);
				}

				if (key_successor[0].equals(myPort) ) {

					suc1[0] = get_only_value(selection);
					Log.i(TAG, " Q: line 984  suc so asking key " + selection + " value : "+suc1[0]) ;
					}

				else {
					Log.i(TAG," Q: line 985  not suc so asking key "+selection+ " from suc "+key_successor[0]);
//					String query_single = "Query_Single" + delimit + selection + delimit + "query_noversion";
				  suc1 = get_query_socket(query_noversion, key_successor[0]);

				}
				if (key_successor[1].equals(myPort) ) {

					suc2[0] = get_only_value(selection);
					Log.i(TAG, " Q: line 998 I am 2nd suc so asking key " + selection + " value : "+suc2[0]) ;
				}
				else {
					Log.i(TAG," Q: line 985  Not 2nd suc  asking key "+selection+ " from suc "+key_successor[0]);
					suc2 = get_query_socket(query_noversion, key_successor[1]);
				}
				Log.i(TAG, " I: line 272, Key previous version key : " + selection + " value in my port is :" + myvalue[0] + " value in suc1 is " + suc1[0] + " value in suc2 is " + suc2[0]);
				String version = get_max_version(myvalue[0], suc1[0], suc2[0]);
				String fileContents = null;
				if ((myvalue[0] != null && !myvalue[0].equals("NOT:FOUND")) && version.equals(myvalue[0].split(delimit_value)[1])) {
					fileContents = myvalue[0].split(delimit_value)[0];
				} else if ((suc1[0] != null && !suc1[0].equals("NOT:FOUND")) && version.equals(suc1[0].split(delimit_value)[1])) {
					fileContents = suc1[0].split(delimit_value)[0];
				} else if ((suc2[0] != null && !suc2[0].equals("NOT:FOUND")) && version.equals(suc2[0].split(delimit_value)[1])) {
					fileContents = suc2[0].split(delimit_value)[0];
				}
				if(fileContents!=null && !fileContents.equals("NOT:FOUND")) {
					matrixcursor.newRow()
							.add("key", selection)
							.add("value", fileContents);
					Log.i(TAG, "Q : line 613;   query: " + selection + " value " + fileContents + " and returned");
				}
			}

		}
		return matrixcursor;
	}

	public MatrixCursor get_value(String key, MatrixCursor matrixcursor){
		StringBuilder text = new StringBuilder();
		try {

			FileInputStream InputStream = getContext().openFileInput(key);


			BufferedReader bufr = new BufferedReader(new InputStreamReader(new BufferedInputStream(InputStream)));

			String line_value;
			while ((line_value = bufr.readLine()) != null) {
				text.append(line_value);
			}

			bufr.close();




		} catch (Exception e) {
			Log.e(TAG, "Q : line 916;   File read failed " + "key = " + key);
		}

		matrixcursor.newRow()
				.add("key", key)
				.add("value", java.lang.String.valueOf(text).split(delimit_value)[0]);
		Log.v(TAG,"Q : line 613;   query: "+ key + " value:  " + java.lang.String.valueOf(text).split(delimit_value)[0]);
		return matrixcursor;
	}
	public MatrixCursor get_versioned_value_cursor(String key, MatrixCursor matrixcursor){
		StringBuilder text = new StringBuilder();
		try {

			FileInputStream InputStream = getContext().openFileInput(key);
			BufferedReader bufr = new BufferedReader(new InputStreamReader(new BufferedInputStream(InputStream)));

			String line_value;
			while ((line_value = bufr.readLine()) != null) {
				text.append(line_value);
			}
			bufr.close();


		} catch (Exception e) {
			Log.e(TAG, "Q : line 916;   File read failed " + "key = " + key);
		}

		matrixcursor.newRow()
				.add("key", key)
				.add("value", text);
		Log.v(TAG,"Q : line 613;   query: "+ key + " " + text);
		return matrixcursor;
	}

	public String get_only_value(String key){
		StringBuilder text = new StringBuilder();
		if(local_key_list.contains(key)){
			try {

				FileInputStream InputStream = getContext().openFileInput(key);


				BufferedReader bufr = new BufferedReader(new InputStreamReader(new BufferedInputStream(InputStream)));

				String line_value;
				while ((line_value = bufr.readLine()) != null) {
					text.append(line_value);
				}

				bufr.close();

			} catch (Exception e) {
				Log.e(TAG, "Q : line 916;   File read failed " + "key = " + key);
			}}
		else {
			text.append("NOT:FOUND");
		}

		return java.lang.String.valueOf(text);
	}




	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			/*
			 * TODO: Fill in your server code that receives messages and passes them
			 * to onProgressUpdate().
			 */
			try {
				while (true) {
					try {

						Socket client = serverSocket.accept();
						Log.d(TAG, "S:  line 361;  Myport  " + myPort);
						BufferedReader br = new BufferedReader(new InputStreamReader(
								client.getInputStream()));
						String r_data;
						if ((r_data = br.readLine()) != null) {
							Log.i(TAG, " S : line 366;  Received msg not null through socket buffer reader is : " + r_data);
							String[] rec_data = r_data.split(delimit);

							if (rec_data[0].equals("IamBack")){

								Log.i(TAG," S: line 900 Sending missed messages to predecessor "+predecessor_port);
								ObjectOutputStream outclient12 = new ObjectOutputStream(client.getOutputStream());
								outclient12.writeObject(predecessor_messages);
//								Thread.sleep(300);
								predecessor_messages.clear();


							}

							if (rec_data[0].equals("Replicate_Message")) {
								Log.i(TAG, " S: line 449, Writing  msg to  content provider for Replication with Key : "+rec_data[2] +" value : "+ rec_data[3]);
								PrintWriter outclient19 = new PrintWriter(client.getOutputStream(),
										true);
								Log.i(TAG, "S: 1230; Acknowledged");
								FileOutputStream outputStream;

								try {
									outputStream = getContext().openFileOutput(rec_data[2], Context.MODE_WORLD_WRITEABLE);
									outputStream.write(rec_data[3].getBytes());
									outputStream.close();
								} catch (Exception e) {
									Log.e(TAG, "I: line 143; File write failed");
								}
								Log.v(TAG,"I : line 145; inserted Key : "+rec_data[2] +" value : "+ rec_data[3]);

								if(!local_key_list.contains(rec_data[2])){
									local_key_list.add(rec_data[2]);
								}

								outclient19.println("received");
								Log.i(TAG, "S: 1230; Acknowledged, stored in local  Replicate message inserted Key : "+rec_data[2] +" value : "+ rec_data[3]);
							}
							if (rec_data[0].equals("Failed_Replicate")) {
								Log.i(TAG, " S: line 449, Writing  msg to  content provider for Replication " + rec_data[3]);
								PrintWriter outclient20 = new PrintWriter(client.getOutputStream(),
										true);
								Log.i(TAG, "S: line 864 writing previous node key for BACKUP and key :"+rec_data[2]);
								predecessor_messages.put(rec_data[3],rec_data[4]);
								outclient20.println("received");
								Log.i(TAG, "S: 1230; Acknowledged BACKUP in Failed Replicate message inserted Key : "+rec_data[3] +" value : "+ rec_data[4]);
							}


							if (rec_data[0].equals("Delete")) {
								delete(uri,rec_data[1],null);
								Log.i(TAG, "S: 479 deleting "+ rec_data[1]+ " values in this node " );
								PrintWriter outclient10 = new PrintWriter(client.getOutputStream(),
										true);
								outclient10.println("received");
							}

							if (rec_data[0].equals("Delete_Replication")) {
								if (local_key_list.contains(rec_data[1])) {
									Log.i(TAG, " D : line 107 : Found query " + rec_data[1] + "and deleted ");
									getContext().deleteFile(rec_data[1]);
									local_key_list.remove(rec_data[1]);
								}
								PrintWriter outclient20 = new PrintWriter(client.getOutputStream(),
										true);
								outclient20.println("received");

//									if(predecessor_messages.containsKey(rec_data[1])){
//										predecessor_messages.remove(rec_data[1]);
//									}

							}


							if (rec_data[0].equals("Query")) {
								HashMap<String,String>pairs_map = new HashMap<String, String>();

								Cursor matrixcursor_local;
								matrixcursor_local= query(uri, null, "@", null, null);
								while(matrixcursor_local.moveToNext()){
									pairs_map.put(matrixcursor_local.getString(matrixcursor_local.getColumnIndex("key")),matrixcursor_local.getString(matrixcursor_local.getColumnIndex("value")));
								}
								ObjectOutputStream outclient12 = new ObjectOutputStream(client.getOutputStream());
								outclient12.writeObject(pairs_map);

							}

							if (rec_data[0].equals("Query_Single")) {
								PrintWriter outclient13 = new PrintWriter(client.getOutputStream(),
										true);
								Log.i(TAG,"S : line 590 In query single, querying :"+rec_data[1]);
								Cursor matrixcursor_single ;

								if(rec_data.length==3){
									String []selection_args={rec_data[2],"Hi"};
									matrixcursor_single= query(uri, null, rec_data[1], selection_args, null);
								}
								else {
									matrixcursor_single= query(uri, null, rec_data[1], null, null);
								}
								matrixcursor_single.moveToFirst();
								Log.i(TAG,  "S line 593; mcs key : " + matrixcursor_single.getString(0)  + " mcs Value : " + matrixcursor_single.getString(1));
								String found_query=matrixcursor_single.getString(1);
								Log.i(TAG, "S: line 1198 Found query " +found_query );
								outclient13.println(found_query);

							}

						}

					} catch (Exception e) {
						Log.e(TAG, "S : line 499 ; Can't listen on ServerSocket " + e.getMessage());
						e.printStackTrace();
					}


					Log.i(TAG," S: 527  Server Running dude ");
				}
			} catch (Exception e) {
				Log.e(TAG, "S: line 507; Can't listen on ServerSocket " + e.getMessage());
				e.printStackTrace();
			}


			return null;
		}

	}
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			if(IamBack.equals("justcame")) {
				String[] key_list = getContext().fileList();
				local_key_list.addAll(Arrays.asList(key_list));
				for(String keys:key_list) {
					Log.i(TAG," C : 1002 key stored in this avd: "+keys );
				}
				try {
					Log.i(TAG, " C : line 848; In try for asking messages to successor port " + successor_port);

					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(successor_port));
					PrintWriter outclient1 = new PrintWriter(socket1.getOutputStream(),
							true);
					outclient1.println("IamBack");
					ObjectInputStream obj = new ObjectInputStream(socket1.getInputStream());
					Object object = null;
					HashMap<String, String> missed_messages = new HashMap<String, String>();
					try {
						object = obj.readObject();
					} catch (Exception e) {
						Log.e(TAG, "C : line 861; Object read failed " + e.getMessage());
						e.printStackTrace();
					}
					if (object != null) {
						Log.i(TAG, " Received Map from suc " + successor_port);
						missed_messages = (HashMap<String, String>) object;
						for (Map.Entry eachpair : missed_messages.entrySet()) {
							String key = (String) eachpair.getKey();
							String value = (String) eachpair.getValue();
							FileOutputStream outputStream;
							try {
								outputStream = getContext().openFileOutput(key, Context.MODE_WORLD_WRITEABLE);
								outputStream.write(value.getBytes());
								outputStream.close();
							} catch (Exception e) {
								Log.e(TAG, "C : line 879; File write failed");
							}
							Log.v(TAG, "C : line 881; insert" + key + " value : " +value);
							if(!local_key_list.contains(key)){
								local_key_list.add(key);
							}

						}
						socket1.close();
					}
				} catch (Exception e) {
					Log.e(TAG, "C : line 892 ;  Exception get messages from nodehash_successor " + e.getMessage());
					e.printStackTrace();
				}



				IamBack=null;
			}

			return null;
		}
	}


}
/*References
https://docs.oracle.com/javase/8/docs/api/java/util/PriorityQueue.html
https://docs.oracle.com/javase/8/docs/api/java/net/Socket.html#setSoTimeout-int-4
https://developer.android.com/reference/java/net/Socket
https://developer.android.com/reference/java/lang/Exception
https://docs.oracle.com/javase/8/docs/api/java/net/Socket.html
https://docs.oracle.com/javase/8/docs/api/java/util/ArrayList.html
https://docs.oracle.com/javase/8/docs/api/java/lang/String.html
https://docs.oracle.com/javase/8/docs/api/java/io/BufferedReader.html
https://docs.oracle.com/javase/8/docs/api/java/io/PrintWriter.html
https://docs.oracle.com/javase/8/docs/api/java/util/TreeMap.html
https://docs.oracle.com/javase/8/docs/api/java/util/List.html
https://developer.android.com/reference/android/database/MatrixCursor.html
 */