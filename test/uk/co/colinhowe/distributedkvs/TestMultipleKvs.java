/**
 * 
 */
package uk.co.colinhowe.distributedkvs;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;

/**
 * @author colin
 *
 */
public class TestMultipleKvs {

  @After
  public void clearKvsRegistration() {
    // Clear out all KVS registrations
    MockCommsLayer.unregisterAll();
  }
  
  /**
   * Ensure that setting data will be shared around a set of
   * nodes. This does not account for a node coming online
   * and taking the data from all existing nodes.
   */
  @Test
  public void setIsShared() {
    new DistributedKvs("kvs1");
    new DistributedKvs("kvs2", "kvs1");
    new DistributedKvs("kvs3", "kvs1");
    
    KvsClient setClient = new KvsClient("client", "kvs1");

    // Put 1000 items in the KVS and they should get shared around
    int counts[] = new int[3];
    for (int i = 0; i < 1000; i++) {
      setClient.set(Integer.toString(i), i);
      
      // Query each KVS for the item
      for (int j = 1; j <= 3; j++) {
        KvsClient getClient = new KvsClient("client", "kvs" + j);
        Integer value = (Integer)getClient.get(Integer.toString(i));
        if (value != null) {
          counts[j - 1]++;
        }
      }
    }
    
    // Output counts for each KVS
    System.out.println("Item counts");
    for (int j = 1; j <= 3; j++) {
      System.out.println("\tKVS " + j + ": " + counts[j - 1]);
    }
    
    // Each kvs should contain items
    assertTrue(counts[0] > 0);
    assertTrue(counts[1] > 0);
    assertTrue(counts[2] > 0);
  }

  /**
   * Ensure that the load is spread to a new node when it comes online.
   */
  @Test
  public void loadSpreadToNewNode() {
    new DistributedKvs("kvs1");
    
    KvsClient setClient = new KvsClient("client", "kvs1");

    // Put 1000 items in the KVS
    for (int i = 0; i < 1000; i++) {
      setClient.set(Integer.toString(i), i);
    }
    
    // Start another KVS
    new DistributedKvs("kvs2", "kvs1");

    // The load should now be shared amongst both KVSs
    int counts[] = new int[3];
    for (int i = 0; i < 1000; i++) {

      // Query each KVS for the item
      for (int j = 1; j <= 2; j++) {
        KvsClient getClient = new KvsClient("client", "kvs" + j);
        Integer value = (Integer)getClient.get(Integer.toString(i));
        if (value != null) {
          counts[j - 1]++;
        }
      }
    }
    
    // Both KVS should now contain items
    assertTrue(counts[0] > 0 && counts[0] < 1000);
    assertTrue(counts[1] > 0);
    
    // Output counts for each KVS
    System.out.println("Item counts");
    for (int j = 1; j <= 3; j++) {
      System.out.println("\tKVS " + j + ": " + counts[j - 1]);
    }
    
    // Start a third KVS... things should get pulled from both KVSs
    new DistributedKvs("kvs3", "kvs1");
    
    int newCounts[] = new int[3];
    for (int i = 0; i < 1000; i++) {

      // Query each KVS for the item
      for (int j = 1; j <= 3; j++) {
        KvsClient getClient = new KvsClient("client", "kvs" + j);
        Integer value = (Integer)getClient.get(Integer.toString(i));
        if (value != null) {
          newCounts[j - 1]++;
        }
      }
    }
    
    // Output counts for each KVS
    System.out.println("Item counts");
    for (int j = 1; j <= 3; j++) {
      System.out.println("\tKVS " + j + ": " + newCounts[j - 1]);
    }
    
    assertTrue(newCounts[0] > 0 && newCounts[0] < counts[0]);
    assertTrue(newCounts[1] > 0 && newCounts[0] < counts[1]);
    assertTrue(newCounts[2] > 0);
  }
}
