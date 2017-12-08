package edu.nyu.adb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Graph {
  private List<Vertex> graph;

  public Graph() {
    graph = new ArrayList<>();
  }

  public void addVertex(int vertexId) {
    graph.add(new Vertex(vertexId));
  }

  // will remove duplicate
  public void addNeighbor(int vertexId, int neighborId) {
    Vertex vertex = getVertex(vertexId);
    Vertex neighbor = getVertex(neighborId);
    if (!vertex.getNeighbors().contains(neighbor)) {
      vertex.addNeighbor(neighbor);
    }
  }

  private Vertex getVertex(int vertexId) {
    for (Vertex vertex : graph) {
      if (vertex.getVertexId() == vertexId) {
        return vertex;
      }
    }
    return null;
  }

  public List<Integer> getNeighbors(int vertexId) {
    Vertex vertex = getVertex(vertexId);
    List<Integer> result = new ArrayList<>();
    for (Vertex temp : vertex.getNeighbors()) {
      result.add(temp.getVertexId());
    }
    return result;
  }

  // remove vertex
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
