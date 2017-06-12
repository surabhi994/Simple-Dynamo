package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

import java.util.Map;

import static java.security.AccessController.getContext;

public class SimpleDynamoActivity extends Activity {
 private  ContentResolver contentResolver;
	Uri uri;
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
		uriBuilder.scheme("content");
		uri = uriBuilder.build();
		contentResolver = getContentResolver();
	final 	TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
		findViewById(R.id.button3).setOnClickListener(

				new OnTestClickListener(tv, getContentResolver()));
		Log.d("checking for","ptest");


		findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View view) {
				Cursor res = contentResolver.query(uri, null,
						"*", null, null);

				if (res.moveToFirst()) {
					while (!res.isAfterLast()) {
						tv.append(res.getString(0) + ":" + res.getString(1) + "\n");
						res.moveToNext();
					}
				} else {
					tv.append("no result\n");
				}
				res.close();

			}
		});

		findViewById(R.id.button1).setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View view) {
				Log.d("checking","activity");
				Cursor res = contentResolver.query(uri, null, "@" + "", null, null);

				if (res.moveToFirst()) {
					while (!res.isAfterLast()) {
						tv.append(res.getString(0) + ":" + res.getString(1) + "\n");
						res.moveToNext();
					}
				} else {
					tv.append("no Result!\n");
				}
				res.close();
			}
		});
		findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View view) {
				Log.d("checking","activity for delete");
				int res = contentResolver.delete(uri, "@", null);
				Log.d("checking", "after delete at activity");
				if (res==0)
				{
					SharedPreferences sharedPref = getApplication().getSharedPreferences("Your Pref", Context.MODE_PRIVATE);
					Map<String, ?> keysforlocal = sharedPref.getAll();

					for (Map.Entry<String, ?> entry : keysforlocal.entrySet())
					{
						Log.d("map values for @delete", entry.getKey() + ": " +
								entry.getValue().toString());
					}
				}
				else
				{
					tv.append("No result\n");
				}

			}
		});
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
