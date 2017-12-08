package edu.nyu.adb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Xi Huang
 */
public class Graph {
  private List<Vertex> graph; // list of vertex

  public Graph() {
    graph = new ArrayList<>();
  }

  public void addVertex(int vertexId) {
    graph.add(new Vertex(vertexId));
  }

  /**
   * add a neighbor for give vertex.
   * @param vertexId the vertex's neighbor you want to add
   * @param neighborId the neighbor you want to add.
   */
  public void addNeighbor(int vertexId, int neighborId) {
    Vertex vertex = getVertex(vertexId);
    Vertex neighbor = getVertex(neighborId);
    if (!vertex.getNeighbors().contains(neighbor)) {
      vertex.addNeighbor(neighbor);
    }
  }

  /**
   * return a vertex for this given vertex ID.
   * @param vertexId
   * @return
   */
  private Vertex getVertex(int vertexId) {
    for (Vertex vertex : graph) {
      if (vertex.getVertexId() == vertexId) {
        return vertex;
      }
    }
    return null;
  }


  /**
   * remove a vertex matching the given id of vertext if it exits in this graph.
   * @param vertexId
   */
  public void removeVertex(int vertexId) {
    int i;
    for (i = 0; i < graph.size(); i ++) {
      if (graph.get(i).getVertexId() == vertexId) {
        break;
      }
    }

    graph.remove(i);

    for (Vertex vertex : graph) {
      vertex.removeNeighbor(vertexId);
    }
  }


  /**
   * detect if there is a cycle in this graph,
   * if exits, return all the vertex in this cycle
   * if not, return a empty list.
   * @return
   */
  public List<Integer> detectDag() {
    int[] visited = new int[graph.size()];

    for (int i = 0; i < visited.length; i ++) {
      visited[i] = 0;
    }

    Set<Integer> result = new HashSet<>();
    for (int i = 0; i < visited.length; i ++) {
      if (visited[i] == 0) {
        dfs(i, graph, visited, result);
      }
    }

    if (result.size() == 0) {
      return new ArrayList<>();
    } else {
      List<Integer> path = new ArrayList<>();
      for(Integer i : result) {
        path.add(graph.get(i).getVertexId());
      }
      return path;
    }

  }

  /**
   * dfs used for detection.
   * @param i the index for vertex in this graph,
   * @param graph
   * @param visited a int array to store the status of every vertex
   * @param result the result to store every vertex in this cycle if cycle exits.
   * @return true, there is a cycle,
   *         false, not.
   */
  private boolean dfs(int i, List<Vertex> graph, int[] visited, Set<Integer> result) {
    if (visited[i] == 1) {
      result.add(i);
      return true;
    }

    Vertex vertex = graph.get(i);
    visited[i] = 1;
    boolean isCycle = false;

    for (Vertex temp : vertex.getNeighbors()) {
      int index = graph.indexOf(temp);
      isCycle = dfs(index, graph, visited, result);

      if (isCycle) {
        if (result.contains(i)) {
          return false;
        } else {
          result.add(i);
          return true;
        }
      }
    }

    visited[i] = 2;
    return isCycle;
  }

}
