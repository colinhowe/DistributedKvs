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
public class TestMockCommsLayer {
  private static class DummyRecipient extends MockCommsLayer.Communicator {
    private int packets = 0;
    
    /**
     * Create a dummy recipient with the given ID.
     * @param id
     */
    public DummyRecipient(final String id) {
      super(id);
    }
    
    /**
     * @see uk.co.colinhowe.distributedkvs.MockCommsLayer.Communicator#receive(java.lang.Object)
     */
    @Override
    public Object receive(Object data) {
      packets++;
      return null;
    }
  }
  
  
  /**
   * Ensure that basic sending/receiving of data works
   */
  @Test
  public void receive() {
    DummyRecipient a = new DummyRecipient("a");
    DummyRecipient b = new DummyRecipient("b");
    DummyRecipient c = new DummyRecipient("c");
    
    a.send("b", null);
    assertEquals(1, b.packets);

    b.send("a", null);
    assertEquals(1, a.packets);

    c.send("b", null);
    assertEquals(2, b.packets);
  }
  
  
  /**
   * Ensure that sending to a dead node throws the correct exception
   */
  @Test
  public void sendToDeadNode() {
    DummyRecipient a = new DummyRecipient("a");
    DummyRecipient b = new DummyRecipient("b");
    
    a.send("b", null);
    assertEquals(1, b.packets);

    b.unregister();
    
    try {
      a.send("b", null);
      fail("Should have thrown exception");
    } catch (IllegalArgumentException e) {
      // All good!
    }
  }
}
