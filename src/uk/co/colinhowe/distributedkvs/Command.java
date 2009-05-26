/**
 * 
 */
package uk.co.colinhowe.distributedkvs;

/**
 * Ugly implementatino of a command.
 * 
 * @author colin
 */
public class Command {
  private final String name;
  private final Object data;

  /**
   * Create the command
   * 
   * @param name
   * @param data
   */
  public Command(String name, Object data) {
    this.name = name;
    this.data = data;
  }
  

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }


  /**
   * @return the data
   */
  public Object getData() {
    return data;
  }
}
