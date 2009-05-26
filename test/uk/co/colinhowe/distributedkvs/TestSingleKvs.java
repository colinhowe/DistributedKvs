/**
 * 
 */
package uk.co.colinhowe.distributedkvs;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author colin
 *
 */
public class TestSingleKvs {

  /**
   * Ensure that basic getting/sending of data works
   */
  @Test
  public void getAndSet() {
    new DistributedKvs("kvs1");
    KvsClient client = new KvsClient("client1", "kvs1");
    
    // Initially no items in the KVS
    assertEquals(null, client.get("foo"));
    
    // Now set and receive the item
    client.set("foo", 42);
    assertEquals(42, client.get("foo"));
  }
}
