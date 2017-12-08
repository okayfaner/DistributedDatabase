package edu.nyu.adb;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Xi Huang
 */
public class Vertex {

  private int vertexId;// vertex id, same as transaction id.
  private List<Vertex> neighbors;// list of vertex as its neighbor

  /**
   * contructor for Vertex.
   * @param vertexId id for the related transaction
   */
  public Vertex(int vertexId) {
    this.vertexId = vertexId;
    neighbors = new ArrayList<>();
  }

  /**
   * return its id.
   * @return
   */
  public int getVertexId() {
    return this.vertexId;
  }

  /**
   * add a vertex to its neighbors list.
   * @param vertex
   */
  public void addNeighbor(Vertex vertex) {
    neighbors.add(vertex);
  }

  /**
   * return its neighbors.
   * @return
   */
  public List<Vertex> getNeighbors() {
    return this.neighbors;
  }

  /**
   * remove a given vertex matching the vertex id from this vertex's neighbors list if it exits.
   * @param vertexId if of the vertex you want to remove.
   */
  public void removeNeighbor(int vertexId) {
    int i;

    for (i = 0; i < neighbors.size(); i ++) {
      if (neighbors.get(i).getVertexId() == vertexId) {
        break;
      }
    }
    if (!neighbors.isEmpty() && i < neighbors.size()) {
      neighbors.remove(i);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hash = 31;
    hash = prime * hash + (vertexId ^ (vertexId >>> 16));
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Vertex other = (Vertex) obj;
    if (vertexId != other.vertexId)
      return false;
    return true;
  }
}
