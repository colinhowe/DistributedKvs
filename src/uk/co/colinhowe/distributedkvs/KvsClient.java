/**
 * 
 */
package uk.co.colinhowe.distributedkvs;


/**
 * @author colin
 *
 */
public class KvsClient extends MockCommsLayer.Communicator {

  private String masterId;
  
  /**
   * @param id
   */
  public KvsClient(String id, String masterId) {
    super(id);
    this.masterId = masterId;
  }
  
  
  
  /**
   * Get the item with the specified key.
   * 
   * @param key
   */
  public Object get(String key) {
    // -- The response is received in the receive method and will be set by the time
    //    send returns...
    //
    return send(masterId, new Command("get", key));
  }
  
  
  /**
   * Set the item with the specified key to the given value.
   * 
   * @param key
   * @param value
   */
  public void set(String key, Object value) {
    send(masterId, new Command("set", new Object[] { key, value }));
  }



  /**
   * @see uk.co.colinhowe.distributedkvs.MockCommsLayer.Communicator#receive(java.lang.Object)
   */
  @Override
  public Object receive(Object data) {
    throw new RuntimeException("Cannot use this method");
  }
}
