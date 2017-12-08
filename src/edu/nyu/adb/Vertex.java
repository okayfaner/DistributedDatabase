package edu.nyu.adb;

import java.util.ArrayList;
import java.util.List;

public class Vertex {

  private int vertexId;
  private List<Vertex> neighbors;

  public Vertex(int vertexId) {
    this.vertexId = vertexId;
    neighbors = new ArrayList<>();
  }

  public int getVertexId() {
    return this.vertexId;
  }

  public void addNeighbor(Vertex vertex) {
    neighbors.add(vertex);
  }

  public List<Vertex> getNeighbors() {
    return this.neighbors;
  }

  public void removeNeighbor(int vertexId) {
    for (Vertex vertex : neighbors) {
      if (vertex.getVertexId() == vertexId) {
        neighbors.remove(vertex);
        break;
      }
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
