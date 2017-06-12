package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static java.lang.String.valueOf;

public class SimpleDynamoProvider extends ContentProvider {
	String myPort="";
	String node_id="";
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private ContentResolver mContentResolver;
	HashMap<String,String> hmPort = new HashMap<String, String>();
	static final int SERVER_PORT = 10000;
	static final String PORT[] = {"11108", "11112", "11116", "11120", "11124"};
	ArrayList<String> node_list= new ArrayList<String>();
	String pref_list[]= new String[2];
	String pred="";
	Uri muri;




	@Override
	public  int delete(Uri uri, String selection, String[] selectionArgs) {

		Log.d("At delete", "node id: " + node_id);
		String hash_key = "";

		SharedPreferences sharedPref = getContext().getSharedPreferences("Your Pref", Context.MODE_PRIVATE);
		try {
			hash_key = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.d("selection key ", selection);
		Log.d("selection hashkey", hash_key);


		if (selection.equals("@"))
		{
			Map<String, ?> keys = sharedPref.getAll();

			for (Map.Entry<String, ?> entry : keys.entrySet()) {
				SharedPreferences.Editor editor = sharedPref.edit();
				editor.remove(entry.getKey());
				editor.apply();
			}
			return 0;

		}

		else {


			Log.d("delete function", "delete" + selection);

			int compare1 = pred.compareTo(hash_key);
			int compare2 = hash_key.compareTo(node_id);
			int compare3= node_id.compareTo(pred);
			int compare4= node_id.compareTo(pref_list[0]);

			if (compare1 < 0 && compare2 <= 0)
			{

				SharedPreferences.Editor editor = sharedPref.edit();
				editor.remove(selection);
				editor.apply();

				Log.d("deleted key :", selection);
				return 0;


			}
			else if ((compare3<0 && compare4<0)&&(compare1<0|| compare2<0))
			{
				Log.d("HelloThere", "delete3.2");

				SharedPreferences.Editor editor = sharedPref.edit();
				editor.remove(selection);
				editor.apply();

				Log.d("deleted key:", selection);
				return 0;

			} else {

				String value = sharedPref.getString(selection,null);
				if (value != null)
				{
					// the key does exist
					SharedPreferences.Editor editor = sharedPref.edit();
					editor.remove(selection);
					editor.apply();

					Log.d("deleted key:", selection);
					return 0;
				} else {
					// handle the value

					String port = "";
					boolean flag = false;
					for (int i = 0; i < node_list.size() - 1; i++) {
						int c1 = node_list.get(i).compareTo(hash_key);
						int c2 = (hash_key).compareTo(node_list.get(i + 1));
						if (c1 < 0 && c2 < 0) {
							port = hmPort.get(node_list.get(i + 1));
							flag = true;
						}
					}
					if (flag == false) {
						port = hmPort.get(node_list.get(0));
					}

					String transfer_msg = "transferDel" + ";" + port + ";" + pref_list[0] + ";" + selection + ";" + "";

					String rec = "";
					Log.d("transfer at delete", transfer_msg);

					try {
						rec = new ClientTask2().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, transfer_msg, port).get();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
					if (rec.equals("0")) {
						Log.d("returned from", "delete");
					}
					return 0;

				}
			}
		}
	}

	@Override
	public String getType(Uri uri) {

		return null;
	}

	@Override
	public  Uri insert(Uri uri, ContentValues values ) {

		String k = values.getAsString("key");
		String hash_key = "";
		String value = values.getAsString("value");


		try {
			hash_key = genHash(k);
		} catch (NoSuchAlgorithmException e1) {
			e1.printStackTrace();
		}


		Log.d("initial insert key ", k);
		Log.d("initial insert hashkey",   hash_key);

		int compare1 = pred.compareTo(hash_key);

		int compare2 = hash_key.compareTo(node_id);
		int compare3= node_id.compareTo(pred);
		int compare4= node_id.compareTo(pref_list[0]);

		Log.d("ComparesValueInsert", Integer.toString(compare1) + ";" + Integer.toString(compare2));
		Log.d("ComparesValueInsert2", k + hash_key);

		if ((compare1 < 0 && compare2 <= 0)) {

			try {

				SharedPreferences sharedPref = getContext().getSharedPreferences("Your Pref", Context.MODE_PRIVATE);
				SharedPreferences.Editor ed = sharedPref.edit();
				ed.putString(k, value);
				ed.apply();
				Log.d("insertingKey -1-", k);
				Log.d("insertingHashedKey", hash_key);

				for(int i=0;i<2;i++)
				{
					String port = hmPort.get(pref_list[i]);
					String transfer_msg = "transferPrefList" + ";" + port + ";" + pref_list[i] + ";" + k + ";" + value;


					Log.d("transfPrefList insert 1", transfer_msg);

					new ClientTask2().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, transfer_msg, port);
				}
			} catch (Exception e) {
				Log.d("insert", "fail");
			}

		} else if ((compare3<0 && compare4<0)&&(compare1<0|| compare2<0)) {

			try {
				SharedPreferences sharedPref = getContext().getSharedPreferences("Your Pref", Context.MODE_PRIVATE);
				SharedPreferences.Editor ed = sharedPref.edit();
				ed.putString(k, value);
				ed.commit();

				Log.d("insertingKey -2-", k);
				Log.d("insertingHashedKey", hash_key);

				for(int i=0;i<2;i++) {
					String port = hmPort.get(pref_list[i]);
					String transfer_msg = "transferPrefList" + ";" + port + ";" + pref_list[i] + ";" + k + ";" + value;


					Log.d("transfPrefList insert 2", transfer_msg);

					new ClientTask2().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, transfer_msg, port);
				}

			} catch (Exception e) {
				Log.d("insert", "fail");
			}

		} else {
			String port=""; String s_port[]= new String[2];
			boolean flag=false; int index=0;
			for(int i=0; i<node_list.size() -1; i++) {
				int c1= node_list.get(i).compareTo(hash_key);
				int c2= (hash_key).compareTo(node_list.get(i+1));
				if(c1<0 && c2<0) {
					port= hmPort.get(node_list.get(i+1));
					index=i+1;
					flag=true;
				}
			}
			if(flag==false) {
				port=hmPort.get(node_list.get(0));
				index=0;
			}
			if(index==3) {
				s_port[0]=hmPort.get(node_list.get(4));
				s_port[1]=hmPort.get(node_list.get(0));
			}
			if(index==4) {
				s_port[0]=hmPort.get(node_list.get(0));
				s_port[1]=hmPort.get(node_list.get(1));
			}
			else if(index==0 || index==1 || index==2) {
				s_port[0]=hmPort.get(node_list.get(index+1));
				s_port[1]=hmPort.get(node_list.get(index+2));
			}

			String transfer_msg = "transferPrefList" + ";" + port + ";" + hash_key + ";" + k + ";" + value;

			Log.d("transfer at insert", transfer_msg);

			new ClientTask2().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, transfer_msg, port);
			for(int i=0;i<2;i++)
			{
				String transfer_msg1 = "transferPrefList" + ";" + s_port[i] + ";" + pref_list[i] + ";" + k + ";" + value;

				Log.d("transfPrefList insert 3", transfer_msg1);

				new ClientTask2().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, transfer_msg1, port);
			}
		}
		return uri;
	}

	@Override
	public boolean onCreate() {


		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		Log.d(" myport", portStr);
		myPort = valueOf((Integer.parseInt(portStr) * 2));
		mContentResolver = getContext().getContentResolver();

		hmPort.put("33d6357cfaaf0f72991b0ecd8c56da066613c089", "11108");
		hmPort.put("208f7f72b198dadd244e61801abe1ec3a4857bc9", "11112");
		hmPort.put("abf0fd8db03e5ecb199a9b82929e9db79b909643", "11116");
		hmPort.put("c25ddd596aa7c81fa12378fa725f706d54325d12", "11120");
		hmPort.put("177ccecaec32c54b82d5aaafc18a2dadb753e3b1", "11124");

		try {
			node_id = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.d("my node id", node_id);

		try {

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {

			Log.e(TAG, "Can't create a ServerSocket");

		}

		node_list.add("177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
		node_list.add("208f7f72b198dadd244e61801abe1ec3a4857bc9");
		node_list.add("33d6357cfaaf0f72991b0ecd8c56da066613c089");
		node_list.add("abf0fd8db03e5ecb199a9b82929e9db79b909643");
		node_list.add("c25ddd596aa7c81fa12378fa725f706d54325d12");
		String msg = "new" + ";" + node_id + ";" + myPort+ ";"+"" + ";" + "";


		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

		return false;
	}

	@Override
	public  Cursor query(Uri uri, String[] projection, String selection,
						 String[] selectionArgs, String sortOrder) {


		Log.d("At query", "node id: " + node_id);

		MatrixCursor cr = new MatrixCursor(new String[]{"key", "value"});


		String hash_key = "";

		SharedPreferences sharedPref = getContext().getSharedPreferences("Your Pref", Context.MODE_PRIVATE);
		try {
			hash_key = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.d("selection key", selection);
		Log.d("selection hashkey", hash_key);
		if (selection.equals("@"))
		{
			Log.d("in query"," @");
			Map<String, ?> keysforlocal = sharedPref.getAll();

			for (Map.Entry<String, ?> entry : keysforlocal.entrySet()) {
				Log.d("map values for @", entry.getKey() + ": " +
						entry.getValue().toString());


				MatrixCursor.RowBuilder builder = cr.newRow();
				builder.add("key", entry.getKey());
				builder.add("value", entry.getValue());

			}//end of for
			cr.setNotificationUri(getContext().getContentResolver(), uri);
			return cr;

		}
		if (selection.equals("*"))
		{ Log.d("in query*","");
			Log.d("myport at starq",myPort);
			int index=0;
			Map<String, ?> keysforlocal = sharedPref.getAll();
			for (Map.Entry<String, ?> entry : keysforlocal.entrySet())
			{

				MatrixCursor.RowBuilder builder = cr.newRow();
				builder.add("key", entry.getKey());
				builder.add("value", entry.getValue());
			}//

			for(int i=0;i<5;i++) {
				if (!(myPort.equals(PORT[i]))) {
					Log.d("in star ","checking");
					Log.d("myport ", myPort);
					Log.d("port[i] ", PORT[i]);

					String rec = "";

					String transfer_msg = "star" + ";" + PORT[i] + ";" + pref_list[0] + ";" + selection + ";" + "";
					try {
						rec=new ClientTask2().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, transfer_msg, myPort).get();//port
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}

					Log.d("got result", "of * query");
					String split[] = rec.split(";");
					String karr[] = split[0].split(" ");
					String v[] = split[1].split(" ");

					int l= karr.length;
					for (int j = 0; j < l; j++) {
						karr[j] = karr[j].replace(",", "").replace("[", "").replace("]", "");
						v[j] = v[j].replace(",", "").replace("[", "").replace("]", "");
					}
					for (int j = 0; j < l; j++) {
						Log.d("key array", karr[j]);
						MatrixCursor.RowBuilder builder = cr.newRow();
						builder.add("key", karr[j]);
						builder.add("value", v[j]);

					}
					Log.d("doing query at *", " key" + karr);
				}

			}

			cr.setNotificationUri(getContext().getContentResolver(), uri);
			Log.d("gdump",myPort);


			return cr;


		}
		else

		{
			Log.d("HelloThere", "query3" + selection);
			String hk = "";
			try {
				hk = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			int compare1 = pred.compareTo(hk);
			int compare2 = hk.compareTo(node_id);
			int compare3= node_id.compareTo(pred);
			int compare4= node_id.compareTo(pref_list[0]);

			if (compare1 < 0 && compare2 <= 0)
			{
				Log.d("doing query", "key" + selection);
				String val = sharedPref.getString(selection, null);
				Log.d("VALRECD", val);
				MatrixCursor.RowBuilder builder = cr.newRow();
				builder.add("key", selection);
				builder.add("value", val);
				cr.setNotificationUri(getContext().getContentResolver(), uri);
				Log.d("CursorMade", "Completed");
				return cr;


			} else if ((compare3<0 && compare4<0)&&(compare1<0|| compare2<0))
			{
				Log.d("doing query"," key"+selection);
				String val = sharedPref.getString(selection, null);
				Log.d("VALRECD", val);
				MatrixCursor.RowBuilder builder = cr.newRow();
				builder.add("key", selection);
				builder.add("value", val);
				cr.setNotificationUri(getContext().getContentResolver(), uri);
				return cr;

			} else {
				Log.d("query will be ","transfered");
				String port="";String rec="";
				boolean flag=false;
				for(int i=0; i<node_list.size() -1; i++)
				{
					int c1= node_list.get(i).compareTo(hash_key);
					int c2= (hash_key).compareTo(node_list.get(i+1));
					if(c1<0 && c2<0)
					{
						port= hmPort.get(node_list.get(i+1));
						flag=true;
					}
				}
				if(flag==false)
				{
					port=hmPort.get(node_list.get(0));
				}
				String transfer_msg = "transQueryPrefList" + ";" + port + ";" + pref_list[0] + ";" + selection + ";" + "";
				try {
					rec = new ClientTask2().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, transfer_msg, myPort).get();//port
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
				Log.d("got result","of transfer query");
				String split[] = rec.split(";");
				String k = split[0];
				String v = split[1];

				MatrixCursor.RowBuilder builder = cr.newRow();
				builder.add("key", k);
				builder.add("value", v);
				Log.d("doing query", " key"+ k);
				Log.d("VALRECD", v);
				cr.setNotificationUri(getContext().getContentResolver(), uri);

				return cr;

			}

		}

	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			String rec="";
			String m[];


			ServerSocket serverSocket = sockets[0];
			Log.d("ServerTask", "Started at port"+ myPort);


			while (true) {
				try {

					Socket s = serverSocket.accept();
					DataInputStream di = new DataInputStream(s.getInputStream());
					DataOutputStream ack = new DataOutputStream(s.getOutputStream());
					rec = di.readUTF();
					m = rec.trim().split(";");

					String type = m[0];

					if (type.equals("new")) {
						ack.writeUTF("ACK");
						s.close();
						Log.d("list", String.valueOf(node_list));
						int index = node_list.indexOf(node_id);
						if (index == 0) {

							pred = node_list.get(4);
							pref_list[0] = node_list.get(1);
							pref_list[1] = node_list.get(2);
							Log.d("checking for ", String.valueOf(index));

						}
						else if (index == 3) {

							pred = node_list.get(index - 1);
							pref_list[0] = node_list.get(index + 1);
							pref_list[1] = node_list.get(0);
							Log.d("checking for ", String.valueOf(index));

						}
						else if (index == 4) {

							pred = node_list.get(index - 1);
							pref_list[0] = node_list.get(0);
							pref_list[1] = node_list.get(1);
							Log.d("checking for ", String.valueOf(index));
						} else  {
							pred = node_list.get(index - 1);
							pref_list[0] = node_list.get(index + 1);
							pref_list[1] = node_list.get(index + 2);
							Log.d("checking for ", String.valueOf(index));
						}



						Log.d("my pred : ", pred);
						Log.d("my succesor 1 : ", pref_list[0]);
						Log.d("my successor 2 : ", pref_list[1]);
					}
					if(type.equals("transferPrefList"))
					{

						publishProgress(rec);
						ack.writeUTF("ACK");
						Thread.sleep(500);
						s.close();
					}
					if(type.equals("star"))
					{
						String selection = "@";
						Log.d("At server *", myPort);
						Uri.Builder uriBuilder = new Uri.Builder();
						uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
						uriBuilder.scheme("content");
						muri = uriBuilder.build();
						Cursor res = query(muri, null, selection, null, null);

						ArrayList<String> keylist= new ArrayList<String>();
						ArrayList<String> vallist= new ArrayList<String>();

						Log.d("cursor","returned");
						Log.d("res length", String.valueOf(res.getCount()));


						for (res.moveToFirst(); !res.isAfterLast(); res.moveToNext()) {

							Log.d("reaching","surRes");
							keylist.add(res.getString(res.getColumnIndex("key")));
							vallist.add(res.getString(res.getColumnIndex("value")));
							Log.d(res.getString(res.getColumnIndex("key")),res.getString(res.getColumnIndex("value")));
						}res.close();

						ack.writeUTF(keylist + ";" + vallist);
						Thread.sleep(1000);
						s.close();


					}
					if(type.equals("transQueryPrefList"))
					{
						Log.d("in", "transferQuery");

						String selection = m[3];
						Uri.Builder uriBuilder = new Uri.Builder();
						uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
						uriBuilder.scheme("content");
						muri = uriBuilder.build();
						Cursor res = query(muri, null, selection, null, null);
						res.moveToFirst();
						Log.d("CursorRecd",res.getString(0));
						int keyIndex = res.getColumnIndex("key");
						int valueIndex = res.getColumnIndex("value");
						String key = res.getString(keyIndex);
						String v = res.getString(valueIndex);
						ack.writeUTF(key + ";" + v);
						Thread.sleep(1000);
						s.close();

					}
					if(type.equals("transferDel"))
					{


						SharedPreferences sharedPref = getContext().getSharedPreferences("Your Pref", Context.MODE_PRIVATE);
						SharedPreferences.Editor editor = sharedPref.edit();
						editor.remove(m[3]);
						editor.apply();

						Log.d("deleted Key at server", m[3]);
						ack.writeUTF("0");
						s.close();
					}
				} catch (Exception e){

				}
			}


		}

		protected void onProgressUpdate(String... strings)
		{
			String rec[]=strings[0].trim().split(";");
			String type=rec[0];

			if(type.equals("transferPrefList"))
			{
				String k=rec[3];
				String value=rec[4];
				SharedPreferences sharedPref = getContext().getSharedPreferences("Your Pref", Context.MODE_PRIVATE);
				SharedPreferences.Editor ed = sharedPref.edit();
				ed.putString(k, value);
				ed.commit();
				Log.d("Inserted at pubprogress", myPort);
				Log.d("inserting_Key "+k, " value "+value);
				Log.d("inserting_Hash "+rec[2] ," value "+ value);

				try {

					Log.d("key"+k, " hashkey" + genHash(k));
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}

			}


		}
	}


	private class ClientTask extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {


			try {
				String rec[]= msgs[0].split(";");


				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(rec[2]));


				String sending_msg = msgs[0];

				DataOutputStream d = new DataOutputStream(socket.getOutputStream());
				d.writeUTF(sending_msg);
				d.flush();
				DataInputStream in = new DataInputStream(socket.getInputStream());
				String s = in.readUTF();
				if (s.equals("ACK")) {
					socket.close();
				}

			}
			catch (IOException e) {

				e.printStackTrace();
			}


			return null;

		}
	}
	private class ClientTask2 extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {


			try {
				String rec[] = msgs[0].split(";");


				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(rec[1]));


				String sending_msg = msgs[0];

				DataOutputStream d = new DataOutputStream(socket.getOutputStream());
				d.writeUTF(sending_msg);
				d.flush();
				DataInputStream in = new DataInputStream(socket.getInputStream());
				String s = in.readUTF();
				if (s.equals("ACK")) {
					socket.close();
				}
				else
				{
					socket.close();
					return s;
				}

			} catch (IOException e) {

				e.printStackTrace();
			}


			return null;

		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {

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
}
