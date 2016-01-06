// DeleteLatencyTest
// Brian Williams
// January 5, 2015
//
// Created with Couchbase-Java-Client-2.2.2

package com.couchbase.support;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class DeleteLatencyTest {
	
	public static void main(String[] args) {
		
		System.out.println("Welcome to DeleteLatencyTest");

		Cluster cluster = CouchbaseCluster.create("10.4.2.121");
		
		Bucket bucket = cluster.openBucket("BUCKETNAME");
		
		JsonObject user = JsonObject.empty()
			    .put("firstname", "Walter")
			    .put("lastname", "White")
			    .put("job", "chemistry teacher")
			    .put("age", 50);
		
		JsonDocument doc = JsonDocument.create("walter", user);

		JsonDocument response = bucket.upsert(doc);

		GetOnly getOnly = new GetOnly();
		
		getOnly.setBucket(bucket);
		
		new Thread(getOnly).start();

		System.out.println("Document created and thread started.  Waiting 5 seconds...");

		// Wait 5 seconds
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		long timeOfDeletion = 0;
		
		// Now delete the document
		System.out.println("Removing document...");
		try {
			bucket.remove("walter");
			timeOfDeletion = System.currentTimeMillis();  // Because of multi-threading this could actually be after the first null is thrown
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception occurred when trying to remove the document.  That should not happen. Exiting.");
			System.exit(1);
		}
		System.out.println("Document removed.  Sleeping 5 seconds.");
		
		// Wait 5 more seconds
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("Finished the post-remove() sleep.");

		if (timeOfDeletion != 0) {
			System.out.println("Time for deletion to take effect was: " + (getOnly.getTimeOfFirstNull() - timeOfDeletion) + "ms. or less.");
		}
		
		// Done with test, stop and clean up
		
		getOnly.stopGoing();
		
		cluster.disconnect();

		System.out.println("Now leaving DeleteLatencyTest");
	}

}

class GetOnly extends Thread {
	
	boolean _keepGoing = true;
	Bucket _bucket = null;

	long timeOfLastSuccess   = 0;
	long timeOfFirstNull     = 0;
	long timeOfLastNull      = 0;
	long timeOfLastException = 0;

	int numberOfSuccess    = 0;
	int numberOfNulls      = 0;
	int numberOfExceptions = 0;
	
	public long getTimeOfLastSuccess()   { return timeOfLastSuccess;   }
	public long getTimeOfFirstNull()     { return timeOfFirstNull;     }
	public long getTimeOfLastNull()      { return timeOfLastNull;      }
	public long getTimeOfLastException() { return timeOfLastException; }
	
	public void stopGoing() {
		_keepGoing = false;
	}
	
	public void setBucket(Bucket b) {
		_bucket = b;
	}
	
	public void run() {
		
		int iteration = 0;
		
		long timeNow = 0;
		
		boolean printStatusEachTime = false;
		boolean sleepBetweenTries = true;
		
		while (_keepGoing) {
			
			if (printStatusEachTime) {
				System.out.println("In GetOnly.  Last Success Time: " + timeOfLastSuccess 
					+ " Last Exception Time: " + timeOfLastException 
					+ " nulls : "     + numberOfNulls 
					+ " iteration: "  + iteration 
					+ " exceptions: " + numberOfExceptions);
			}
			
			timeNow = System.currentTimeMillis();
			
			if (_bucket != null ) {
				try {
					JsonDocument walter = _bucket.get("walter");
					if (walter != null) {
						timeOfLastSuccess = timeNow;
						numberOfSuccess++;
					}
					else {
						// System.out.println("No exception, but got null");
						if (timeOfFirstNull == 0) { timeOfFirstNull = timeNow; }
						timeOfLastNull = timeNow;
						numberOfNulls++;
					}
				} catch(Exception e) {
					timeOfLastException = timeNow;
					numberOfExceptions++;
				}
			}
			else {
				System.out.println("bucket is null.  Cannot do a get()");
			}
			
			if (sleepBetweenTries) {
			// Sleep between tries, you may experiment with different values here
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			iteration++;
			
		} // while keep going....

		System.out.println("GetOnly: ceasing operation, total iterations: " + iteration + " total nulls: " + numberOfNulls + " total exceptions: " + numberOfExceptions);

	}
	
}

// EOF