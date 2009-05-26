/**
 * 
 */
package uk.co.colinhowe.distributedkvs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

/**
 * @author colin
 *
 */
public class DistributedKvs extends MockCommsLayer.Communicator {
  private static final int MAX_SHARDS = 100;

  // Just a simple hashmap to store the data.
  // The store isn't the interesting bit, the distribution is
  private Map<Integer, Map<String, Object>> stores = new HashMap<Integer, Map<String, Object>>();
  private String[] shardingTable = new String[MAX_SHARDS];
  
  /**
   * @param id
   */
  public DistributedKvs(String id) {
    super(id);
    
    // Initially everything is on this KVS.
    for (int i = 0; i < MAX_SHARDS; i++) {
      shardingTable[i] = this.getId();
      stores.put(i, new HashMap<String, Object>());
    }
  }
  
  
  /**
   * @param id
   */
  @SuppressWarnings("unchecked")
  public DistributedKvs(String id, String existingKvs) {
    this(id);
    
    // Get the existing hash->KVS map
    shardingTable = (String[])send(existingKvs, new Command("getShardTable", null));
    
    // Work out how much of the load we want to take by counting up how many other 
    // KVS are online
    Set<String> uniqueIds = new HashSet<String>();
    for (String kvsId : shardingTable) {
      uniqueIds.add(kvsId);
    }
    int shardsToTake = MAX_SHARDS / (uniqueIds.size() + 1);
    
    // -- Inform the existing KVS that we want to take on some of the load.
    //    Currently this is a total random strategy, not ideal but it does the job...
    //
    
    List<Integer> potentialShards = new LinkedList<Integer>();
    for (int i = 0; i < MAX_SHARDS; i++) {
      potentialShards.add(i);
    }
    
    
    Random random = new Random();
    for (int i = 0; i < shardsToTake; i++) {
      int shard = potentialShards.remove(random.nextInt(potentialShards.size()));
      String existingShardOwner = shardingTable[shard];
      shardingTable[shard] = getId();
      
      List<Object[]> values = (List<Object[]>)send(existingShardOwner, new Command("shareLoad", new Object[] { id, shard }));
      for (Object[] keyValue : values) {
        stores.get(shard).put((String)keyValue[0], keyValue[1]);
      }
    }
  }
  
  
  /**
   * @see uk.co.colinhowe.distributedkvs.MockCommsLayer.Communicator#receive(java.lang.Object)
   */
  @Override
  public Object receive(Object data) {
    Command command = (Command)data;
    
    if (command.getName().equals("get")) {
      return stores.get((Integer)getShardIndex(command.getData())).get(command.getData());
    } else if (command.getName().equals("set")) {
      set(command.getData());
    } else if (command.getName().equals("getShardTable")) {
      return getShardTable();
    } else if (command.getName().equals("shareLoad")) {
      return updateShardTable(command.getData());
    }
    
    return null;
  }
  
  
  /**
   * Processes a command to update the shard table
   * 
   * @param args
   */
  private List<Object[]> updateShardTable(Object args) {
    Object[] argArray = (Object[])args;
    String kvsId = (String)argArray[0];
    Integer shardIndex = (Integer)argArray[1];
    
    shardingTable[shardIndex] = kvsId;
    
    // Get all the items from this shard on this instance
    List<Object[]> values = new ArrayList<Object[]>();
    for (Entry<String, Object> keyValue : stores.get(shardIndex).entrySet()) {
      values.add(new Object[] { keyValue.getKey(), keyValue.getValue() } );
    }
    
    // Now remove the items from this shard
    stores.get(shardIndex).clear();
    
    return values;
  }
  

  /**
   * Processes a set command.
   * 
   * @param args
   */
  private void set(Object args) {
    Object[] commandData = (Object[])args;
    
    String key = (String)commandData[0];
    Object value = commandData[1];

    // If this item is in this KVS then set it here...
    // Otherwise, delegate to the relevant KVS
    int shardIndex = getShardIndex(key);
    if (shardingTable[shardIndex].equals(getId())) {
      stores.get(shardIndex).put(key, value);
    } else {
      send(shardingTable[shardIndex], new Command("set", args));
    }
  }
  
  
  /**
   * Get the shard index for the given key
   * 
   * @param key
   * @return
   */
  private int getShardIndex(Object key) {
    return key.hashCode() % MAX_SHARDS;
  }
  
  
  /**
   * Get the shard table
   * @return
   */
  private String[] getShardTable() {
    String[] copy = new String[shardingTable.length];
    for (int i = 0; i < copy.length; i++) {
      copy[i] = shardingTable[i];
    }
    return copy;
  }
}
