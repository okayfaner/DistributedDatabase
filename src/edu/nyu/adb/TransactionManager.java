package edu.nyu.adb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

public class TransactionManager {

  private File file;
  private BufferedReader bufferedReader;
  private HashMap<Integer, List<Integer>> variableIdToSiteId;
  private HashMap<Integer, Site> siteIdToSite;
  private HashMap<Integer, Transaction> transactionIdToTransaction;
  private HashMap<Integer, HashSet<Integer>> transactionIdToSites;
  private List<Operation> waitlistOperation;
  private Graph graph;

  public TransactionManager(String inputFilePath) {
    variableIdToSiteId = new HashMap<>();
    for (int i = 1; i <= 20; i++) {
      variableIdToSiteId.putIfAbsent(i, new ArrayList<>());
      if (i % 2 == 0) {
        for (int j = 1; j <= 10; j++) {
          variableIdToSiteId.get(i).add(j);
        }
      } else {
        variableIdToSiteId.get(i).add(i % 10 + 1);
      }
    }
    siteIdToSite = new HashMap<>();
    for (int i = 1; i <= 10; i++) {
      siteIdToSite.put(i, new Site(i));
    }
    transactionIdToTransaction = new HashMap<>();
    transactionIdToTransaction = new HashMap<>();
    transactionIdToSites = new HashMap<>();
    waitlistOperation = new ArrayList<>();
    graph = new Graph();
    try {
      file = new File(inputFilePath);
      bufferedReader = new BufferedReader(new FileReader(file));
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  public void init() {
    try {

      while (true) {
        String line = bufferedReader.readLine();
        if (line == null) break;
        else if (line.length() != 0) parseLine(line);
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  private void parseLine(String line) {
    String command = line.trim();

    if (command.startsWith("beginRO")) {
      String transactionId = command.substring(command.indexOf("T") + 1, command.indexOf(")"));
      startTransaction("RO", Integer.parseInt(transactionId));
    }
    else if (command.startsWith("begin")) {
      String transactionId = command.substring(command.indexOf("T") + 1, command.indexOf(")"));
      startTransaction("RW", Integer.parseInt(transactionId));
    }
    else if (command.startsWith("end")) {
      int transactionId = Integer.valueOf(command.substring(command.indexOf("T") + 1, command.indexOf(")")));
      if (transactionIdToTransaction.containsKey(transactionId)) {
        endTransaction(transactionId);
      }
    }
    else if (command.startsWith("fail")) {
      String siteId = command.substring(command.indexOf("(") + 1, command.indexOf(")"));
      failSite(Integer.parseInt(siteId));
    }
    else if (command.startsWith("recover")) {
      String siteId = command.substring(command.indexOf("(") + 1, command.indexOf(")"));
      recoverSite(Integer.parseInt(siteId));
    }
    else if (command.startsWith("dump")) {
      if (command.indexOf("(") == command.indexOf(")") - 1) dump();
      else if (command.indexOf("x") != -1) {
        dump(command.substring(command.indexOf("("), command.indexOf(")")));
      }
      else {
        dump(Integer.parseInt(command.substring(command.indexOf("("), command.indexOf(")"))));
      }
    }
    else if (command.startsWith("R")) {
      int transactionId = Integer.parseInt(command.substring(command.indexOf("T") + 1, command.indexOf(",")));
      String variableId = command.substring(command.indexOf("x") + 1, command.indexOf(")"));
      if (transactionIdToTransaction.containsKey(transactionId)) {
        addSiteIdToTransactionMap(transactionId, Integer.parseInt(variableId));
        readOperation(transactionId, Integer.parseInt(variableId));
      }
    }
    else if (command.startsWith("W")) {
      String[] parameters = command.substring(command.indexOf("(") + 1, command.indexOf(")")).split(",");
      String transactionId = parameters[0].substring(1);
      String variableId = parameters[1].substring(1);
      String newValue = parameters[2];
      if (transactionIdToTransaction.containsKey(transactionId)) {
        addSiteIdToTransactionMap(Integer.parseInt(transactionId), Integer.parseInt(variableId));
        writeOperation(Integer.parseInt(transactionId), Integer.parseInt(variableId), Integer.parseInt(newValue));
      }
    }
    else {
      System.err.println("Error input file format.");
    }
  }

  private void addSiteIdToTransactionMap(int transactionId, int variableId) {
    if (variableId % 2 == 1) {
      transactionIdToSites.putIfAbsent(transactionId, new HashSet<>());
      transactionIdToSites.get(transactionId).add(variableId % 10 + 1);
    }
    else {
      transactionIdToSites.putIfAbsent(transactionId, new HashSet<>());
      for (int i = 1; i <= 10; i++) {
        transactionIdToSites.get(transactionId).add(i);
      }
    }
  }

  private void startTransaction(String type, int transactionId) {
    System.out.println();
    System.out.println("Starting transaction " + transactionId);
    if (type.equals("RW")) {
      transactionIdToTransaction.put(transactionId, new Transaction(transactionId, new Date(), Transaction.TranType.RW));
      graph.addVertex(transactionId);
    }
    else {
      transactionIdToTransaction.put(transactionId, new Transaction(transactionId, new Date(), Transaction.TranType.RO));
    }
    System.out.println();
  }

  private void endTransaction(int transactionId) {
    if (!transactionIdToTransaction.containsKey(transactionId)) {
      graph.removeVertex(transactionId);
      return;
    }

    boolean flagForRemoveVertex = true;
    int index = -1;
    for (int i = 0; i < waitlistOperation.size(); i++) {
      if (waitlistOperation.get(i).getTransId() == transactionId) {
        index = i;
      }
    }
    // if the transaction we want to end exists in waiting list, we try to end it.
    // If succeed, remove it from waitlist, try end every transaction being blocked by it, else keep it in waitlist.
    if (index != -1) {
      Operation operation = waitlistOperation.get(index);
      if (!runOperation(true, operation)) {
        flagForRemoveVertex = false;
      }
      else {
        waitlistOperation.remove(index);
      }
    }
    // we end all operations of the transaction. If it blocked, add it to waitlist.
    // if succeed, we try to end all the neighbors of this transanction
    else {
      Transaction transaction = transactionIdToTransaction.get(transactionId);
      List<Operation> operationList = transaction.getOperations();

      // iterate through the whole operation list of the transaction to commit all operations
      for (Operation operation : operationList) {
        if (!runOperation(false, operation)) {
          waitlistOperation.add(operation);
          flagForRemoveVertex = false;
          break; // Actually, do not need to break; since the blocked one can only be the last one in the transaction
        }
      }
    }
    if (flagForRemoveVertex) {
      List<Integer> neighbors = graph.getNeighbors(transactionId);
      graph.removeVertex(transactionId);
      transactionIdToTransaction.remove(transactionId);
      for (int neighbor : neighbors) {
        endTransaction(neighbor);
      }
    }
  }
  /*
  private void endTransaction(int transactionId) {
    if (!transactionIdToTransaction.containsKey(transactionId)) {
      graph.removeVertex(transactionId);
      return;
    }

    boolean flagForRemoveVertex = true;
    int index = -1;
    for (int i = 0; i < waitlistOperation.size(); i++) {
      if (waitlistOperation.get(i).getTransId() == transactionId) {
        index = i;
      }
    }
    // if the transaction we want to end exists in waiting list, we try to end it.
    // If succeed, remove it from waitlist, try end every transaction being blocked by it, else keep it in waitlist.
    if (index != -1) {
      Operation operation = waitlistOperation.get(index);
      // if it's read operation
      if (operation.getType() == Operation.OpType.read) {
        int variableId = operation.getVariableIndex();
        if (variableId % 2 == 1) {
          Site site = siteIdToSite.get(variableId % 10 + 1);
          if (!site.commit(operation, transaction)) {
            flagForRemoveVertex = false;
            waitlistOperation.add(operation);
          }
        } else {
          boolean flag = true;
          for (int i = 1; i <= 10; i++) {
            Site site = siteIdToSite.get(i);
            if (site.commit(operation, transaction)) {
              flag = false;
              break;
            }
          }
          if (flag) {
            flagForRemoveVertex = false;
            waitlistOperation.add(operation);
          }
        }
      }
      // if it's write operation
      else {

      }
      waitlistOperation.remove(index);
    }
    // we end all operations of the transaction. If it blocked, add it to waitlist.
    // if succeed, we try to end all the neighbors of this transanction
    else {
      Transaction transaction = transactionIdToTransaction.get(transactionId);
      List<Operation> operationList = transaction.getOperations();

      // iterate through the whole operation list of the transaction to commit all operations
      for (Operation operation : operationList) {
        // if it's read operation
        if (operation.getType() == Operation.OpType.read) {
          int variableId = operation.getVariableIndex();
          if (variableId % 2 == 1) {
            Site site = siteIdToSite.get(variableId % 10 + 1);
            if (site.commit(operation, transaction)) {
              continue;
            } else {
              flagForRemoveVertex = false;
              waitlistOperation.add(operation);
            }
          } else {
            boolean flag = true;
            for (int i = 1; i <= 10; i++) {
              Site site = siteIdToSite.get(i);
              if (site.commit(operation, transaction)) {
                flag = false;
                break;
              }
            }
            if (flag) {
              flagForRemoveVertex = false;
              waitlistOperation.add(operation);
            }
          }
        }
        // if it's write operation
        else {

        }
      }
    }

    if (flagForRemoveVertex) {
      // neighbors are blocked transaction ids
      List<Integer> neighbors = graph.getNeighbors(transactionId);
      graph.removeVertex(transactionId);
      transactionIdToTransaction.remove(transactionId);
      for (int neighbor : neighbors) {
        endTransaction(transactionId);
      }
    }
  }
  */

  private boolean runOperation(boolean inWaitList, Operation operation) {
    // if it's read operation, can not run on failed or recovery site.
    if (operation.getType() == Operation.OpType.read) {
      int variableId = operation.getVariableIndex();
      Transaction transaction = transactionIdToTransaction.get(operation.getTransId());
      if (variableId % 2 == 1) {
        Site site = siteIdToSite.get(variableId % 10 + 1);
        if (site.commit(operation, transaction)) {
          return true;
        }
        else {
          return false;
        }
      } else {
        for (int i = 1; i <= 10; i++) {
          Site site = siteIdToSite.get(i);
          if (site.commit(operation, transaction)) {
            return true;
          }
        }
        return false;
      }
    }
    // if it's write operation, can not run on failed site but can run on recovery site.
    else {
      // TODO:
    }
  }

  private void failSite(int siteId) {
    System.out.println();
    System.out.println("Failliing site " + siteId);
    Site site = siteIdToSite.get(siteId);
    List<Integer> transactionIdsOnTheSite = site.fail();
    for (int transactionId : transactionIdsOnTheSite) {
      abort(transactionIdToTransaction.get(transactionId));
    }
    /*
    for (int transactionId : transactionIdsOnTheSite) {
      // Abort transaction by removing from multiple places
      transactionIdToTransaction.remove(transactionId);
      graph.removeVertex(transactionId);
      List<Integer> removeIndex = new ArrayList<>();
      for (int i = 0; i < waitlistOperation.size(); i++) {
        if (operation.getTransId() == transaction.getTransactionId()) {
          removeIndex.add(i);
        }
      }
      for (int index : removeIndex) {
        waitlistOperation.remove(index);
      }
    }
    for (int siteIdForAbortTransaction : siteIdToSite.keySet()) {
      Site siteForAbortTransaction = siteIdToSite.get(siteIdForAbortTransaction);
      for (int transactionId : transactionIdsOnTheSite) {
        if (siteForAbortTransaction.removeTransactions(transactionId)) {
          System.out.println("Aborting transaction " + transactionId + " at site " + siteIdForAbortTransaction);
        }
      }
    }
    */
    System.out.println("Site " + siteId + " failed");
    System.out.println();
  }

  private void recoverSite(int siteId) {
    System.out.println();
    System.out.println("Recovering site " + siteId);
    Site site = siteIdToSite.get(siteId);
    site.recover();
    for (Operation waitingOperation : waitlistOperation) {
      if (waitingOperation.getType() == Operation.OpType.read) {

      }
    }
    System.out.println("Site " + siteId + " recovered");
    System.out.println();
  }

  // dump all site with all variables
  private void dump() {
    for (int i = 1; i <= 10; i++) {
      dump(i);
    }
  }

  // dump all site with a specific variable
  private void dump(String variableId) {
    List<Integer> siteIds = variableIdToSiteId.get(variableId);
    System.out.println();
    System.out.println("=== output of dump variable " + variableId);
    for (int siteId : siteIds) {
      Site site = siteIdToSite.get(siteId);
      System.out.print("Site: " + siteId);
      site.dump(Integer.parseInt(variableId));
      System.out.println();
    }
  }

  // dump all variable in a specific site
  private void dump(int siteId) {
    Site site = siteIdToSite.get(siteId);
    System.out.println();
    System.out.println("=== output of dump site " + siteId);
    site.dump();
    System.out.println();
  }

  public void readOperation(int transactionId, int variableId) {
    if (!transactionIdToTransaction.containsKey(transactionId)) {
      System.err.println();
      System.err.println("Aborted transaction trying to perform read operation");
      return;
    }
    Transaction transaction = transactionIdToTransaction.get(transactionId);
    // Add read operation of read only transaction
    if (transaction.getType() == Transaction.TranType.RO) {
      Date operationTimestamp = transaction.getTimeStamp();
      // if odd indexed variable
      if (variableId % 2 == 1) {
        Site site = siteIdToSite.get(variableId % 10 + 1);
        Operation operation = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
        site.addOperation(transactionId, operation);
        Operation operation1 = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
        transactionIdToTransaction.get(transactionId).addOperations(operation1);
      }
      // if even indexed variable
      else {
        List<Integer> siteIds = variableIdToSiteId.get(variableId);
        for (int siteId : siteIds) {
          Site site = siteIdToSite.get(siteId);
          Operation operation = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
          site.addOperation(transactionId, operation);
        }
        Operation operation1 = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
        transactionIdToTransaction.get(transactionId).addOperations(operation1);
      }
    }
    // Add read operation of read write transaction
    else {
      Date operationTimestamp = new Date();
      // if odd indexed variable, only one site
      if (variableId % 2 == 1) {
        Site site = siteIdToSite.get(variableId % 10 + 1);
        site.addLock(variableId, new Lock(variableId, transactionId, Lock.lockType.READ));

        // Add edge to graph by iterating lock table
        List<Lock> lockTable = site.getLockTable(variableId);
        for (Lock lock : lockTable) {
          if (lock.getType() == Lock.lockType.WRITE && lock.getTranscId() != transactionId) {
            graph.addNeighbor(lock.getTranscId(), transactionId);
          }
        }

        Operation operation = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
        site.addOperation(transactionId, operation);
        Operation operation1 = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
        transactionIdToTransaction.get(transactionId).addOperations(operation1);
        deadlockDetectAndAbort();
      }
      // if even indexed variable, multiple site
      else {
        List<Integer> siteIds = variableIdToSiteId.get(variableId);
        for (int siteId : siteIds) {
          Site site = siteIdToSite.get(siteId);
          site.addLock(variableId, new Lock(variableId, transactionId, Lock.lockType.READ));

          // Add edge to graph by iterating lock table
          List<Lock> lockTable = site.getLockTable(variableId);
          for (Lock lock : lockTable) {
            if (lock.getType() == Lock.lockType.WRITE && lock.getTranscId() != transactionId) {
              graph.addNeighbor(lock.getTranscId(), transactionId);
            }
          }

          Operation operation = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
          site.addOperation(transactionId, operation);
        }

        Operation operation1 = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
        transactionIdToTransaction.get(transactionId).addOperations(operation1);
        deadlockDetectAndAbort();
      }
    }
  }

  private void writeOperation(int transactionId, int variableId, int newValue) {
    Date operationTimestamp = new Date();
    // if odd indexed variable, only one site
    if (variableId % 2 == 1) {
      Site site = siteIdToSite.get(variableId % 10 + 1);
      site.addLock(variableId, new Lock(variableId, transactionId, Lock.lockType.WRITE));

      // Add edge to graph by iterating lock table
      List<Lock> lockTable = site.getLockTable(variableId);
      for (Lock lock : lockTable) {
        if (lock.getTranscId() != transactionId) {
          graph.addNeighbor(lock.getTranscId(), transactionId);
        }
      }

      Operation operation = new Operation(Operation.OpType.write, variableId, newValue, operationTimestamp, transactionId);
      site.addOperation(transactionId, operation);
      Operation operation1 = new Operation(Operation.OpType.write, variableId, newValue, operationTimestamp, transactionId);
      transactionIdToTransaction.get(transactionId).addOperations(operation1);
      deadlockDetectAndAbort();
    }
    // if even indexed variable, multiple site
    else {
      List<Integer> siteIds = variableIdToSiteId.get(variableId);
      for (int siteId : siteIds) {
        Site site = siteIdToSite.get(siteId);
        site.addLock(variableId, new Lock(variableId, transactionId, Lock.lockType.WRITE));

        // Add edge to graph by iterating lock table
        List<Lock> lockTable = site.getLockTable(variableId);
        for (Lock lock : lockTable) {
          if (lock.getTranscId() != transactionId) {
            graph.addNeighbor(lock.getTranscId(), transactionId);
          }
        }

        Operation operation = new Operation(Operation.OpType.write, variableId, newValue, operationTimestamp, transactionId);
        site.addOperation(transactionId, operation);
      }

      Operation operation1 = new Operation(Operation.OpType.write, variableId, newValue, operationTimestamp, transactionId);
      transactionIdToTransaction.get(transactionId).addOperations(operation1);
      deadlockDetectAndAbort();
    }
  }

  private void deadlockDetectAndAbort() {
    List<Integer> deadLockTransactionIds = graph.detectDag();
    while (!deadLockTransactionIds.isEmpty()) {
      Transaction transaction = transactionIdToTransaction.get(deadLockTransactionIds.get(0));
      for (int i = 1; i < deadLockTransactionIds.size(); i++) {
        Transaction transaction1 = transactionIdToTransaction.get(deadLockTransactionIds.get(i));
        transaction = transaction.getTimeStamp().after(transaction1.getTimeStamp()) ? transaction : transaction1;
      }
      abort(transaction);
      deadLockTransactionIds = graph.detectDag();
    }
  }

  private void abort(Transaction transaction) {
    // remove from graph
    graph.removeVertex(transaction.getTransactionId());
    // remove from transaction map
    transactionIdToTransaction.remove(transaction.getTransactionId());
    // remove from sites
    for (int siteId : transactionIdToSites.get(transaction.getTransactionId())) {
      Site site = siteIdToSite.get(siteId);
      site.removeTransactions(transaction.getTransactionId());
    }
    // remove from waitlist Operation
    List<Integer> removeIndex = new ArrayList<>();
    for (int i = 0; i < waitlistOperation.size(); i++) {
      if (waitlistOperation.get(i).getTransId() == transaction.getTransactionId()) {
        removeIndex.add(i);
      }
    }
    for (int index : removeIndex) {
      waitlistOperation.remove(index);
    }
  }

}
