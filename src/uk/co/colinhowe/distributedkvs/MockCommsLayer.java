/**
 * 
 */
package uk.co.colinhowe.distributedkvs;

import java.util.HashMap;
import java.util.Map;

/**
 * @author colin
 *
 */
public class MockCommsLayer {
  private static Map<String, Communicator> communicators = new HashMap<String, Communicator>();
  
  public static abstract class Communicator {
    private final String id;
    
    
    /**
     * Called when some data has been sent to this communicator.
     * 
     * @param data
     * @return the response
     */
    public abstract Object receive(Object data);

    
    /**
     * Create the communicator
     */
    public Communicator(String id) {
      this.id = id;
      this.register();
    }
    

    /**
     * Get the ID of this communicator
     * 
     * @return
     */
    public String getId() {
      return id;
    }
    
    /**
     * Register the communicator
     */
    public void register() {
      communicators.put(getId(), this);
    }
    
    public void unregister() {
      communicators.remove(getId());
    }
    
    
    /**
     * Send the data to the given communicator with the specified ID.
     * 
     * @param id communicator to send to
     * @param data payload
     * @return the response from the recipient
     * @throws IllegalArgumentException if the recipient is not registered
     */
    public Object send(String id, Object data) {
      Communicator recipient = communicators.get(id);
      if (recipient == null) {
        throw new IllegalArgumentException("Recipient has gone");
      }
      
      return recipient.receive(data);
    }
  }

  /**
   * Clears all registered communicators
   */
  public static void unregisterAll() {
    communicators.clear();
  }
}
