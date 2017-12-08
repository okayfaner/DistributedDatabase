package edu.nyu.adb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Oukan Fan
 */
public class TransactionManager {

  private File file;// input file for read.
  private BufferedReader bufferedReader;
  private HashMap<Integer, List<Integer>> variableIdToSiteId;// <variable , sites containing variable>
  private HashMap<Integer, Site> siteIdToSite;// < siteId, site>
  private HashMap<Integer, Transaction> transactionIdToTransaction;// <transaction ID, Transaction>
  private HashMap<Integer, HashSet<Integer>> transactionIdToSites;// <transaction ID, Set of sites ID>
  private List<Operation> waitlistOperation; // waitlist for operations.
  private Graph graph;// graph used for deadlock detection.

  /**
   * construction for TM
   * @param inputFilePath the path for input file containing database commands.
   */
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
    waitlistOperation = new CopyOnWriteArrayList<>();
    graph = new Graph();
    try {
      file = new File(inputFilePath);
      bufferedReader = new BufferedReader(new FileReader(file));
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  /**
   * init TM.
   */
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

  /**
   * parse every line and call related methods.
   * @param line input string.
   */
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
        dump(command.substring(command.indexOf("(") + 2, command.indexOf(")")));
      }
      else {
        dump(Integer.parseInt(command.substring(command.indexOf("(") + 1, command.indexOf(")"))));
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
      String transactionId = parameters[0].trim().substring(1);
      String variableId = parameters[1].trim().substring(1);
      String newValue = parameters[2].trim();
      if (transactionIdToTransaction.containsKey(Integer.parseInt(transactionId))) {
        addSiteIdToTransactionMap(Integer.parseInt(transactionId), Integer.parseInt(variableId));
        writeOperation(Integer.parseInt(transactionId), Integer.parseInt(variableId), Integer.parseInt(newValue));
      }
    }
    else {
      System.err.println("Error input file format.");
    }
  }

  /**
   * Add related site ID to a given transaction
   * @param transactionId id for transaction
   * @param variableId id for variable
   */
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

  /**
   * start transaction for parse "begin" and "beginRO"
   * @param type string for transaction type
   * @param transactionId id for transaction
   */
  private void startTransaction(String type, int transactionId) {
    System.out.println();
    System.out.println("Starting transaction " + transactionId);
    if (type.equals("RW")) {
      transactionIdToTransaction.put(transactionId, new Transaction(transactionId, System.nanoTime(), Transaction.TranType.RW));
      graph.addVertex(transactionId);
    }
    else {
      transactionIdToTransaction.put(transactionId, new Transaction(transactionId, System.nanoTime(), Transaction.TranType.RO));
      graph.addVertex(transactionId);
    }
    System.out.println();
  }

  /**
   * try to end this transaction,
   * if success, it will output info for this commit, and remove this transaction from map and its related vertex from graph.
   * it not, the operation causing this will be added to operation waitlist.
   * @param transactionId
   */
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
    // we end all operations of the transaction. If it blocked, add it to waitlist.
    // if succeed, we try to end all the neighbors of this transanction
    if (index == -1) {
      Transaction transaction = transactionIdToTransaction.get(transactionId);
      List<Operation> operationList = transaction.getOperations();
      transaction.removeAllOperations();

      // iterate through the whole operation list of the transaction to commit all operations
      for (Operation operation : operationList) {
        int variableId = operation.getVariableIndex();

        // for read to release lock
        if (operation.getType() == Operation.OpType.read && transaction.getType() == Transaction.TranType.RW) {
          if (variableId % 2 == 1) {
            Site site = siteIdToSite.get(variableId % 10 + 1);
            site.dropLock(variableId, transactionId);
          } else {
            for (int i = 1; i <= 10; i ++) {
              siteIdToSite.get(i).dropLock(variableId, transactionId);
            }
          }
        }

        if (operation.getType() == Operation.OpType.write && transaction.getType() == Transaction.TranType.RW) {
          runOperation(operation);
        }
      }
    }
    // if the transaction we want to end exists in waiting list, we try to end it.
    // If succeed, remove it from waitlist, try end every transaction being blocked by it, else keep it in waitlist.
    else {
      Transaction transaction = transactionIdToTransaction.get(transactionId);
      List<Operation> operationList = transaction.getOperations();
      transaction.removeAllOperations();
      for (Operation operation : operationList) {
        int variableId = operation.getVariableIndex();

        if (operation.getType() == Operation.OpType.read && transaction.getType() == Transaction.TranType.RW) {
          if (variableId % 2 == 1) {
            Site site = siteIdToSite.get(variableId % 10 + 1);
            site.dropLock(variableId, transactionId);
          } else {
            for (int i = 1; i <= 10; i ++) {
              siteIdToSite.get(i).dropLock(variableId, transactionId);
            }
          }
        }

        if (operation.getType() == Operation.OpType.write && transaction.getType() == Transaction.TranType.RW) {
          runOperation(operation);
        }
      }

      Operation operation = waitlistOperation.get(index);
      if (!runOperation(operation)) {
        flagForRemoveVertex = false;
      }
      else {
        waitlistOperation.remove(index);
      }
    }
    if (flagForRemoveVertex) {
      boolean flag = transactionIdToTransaction.get(transactionId).getType() == Transaction.TranType.RW;
      if (flag) {
        graph.removeVertex(transactionId);
      }
      System.out.println();
      System.out.println("T" + transactionId + " Commit, due to " +
              (index == -1 ? "normal commit" : "being unblocked and executed"));
      System.out.println();
      transactionIdToTransaction.remove(transactionId);
      for(Integer temp : transactionIdToSites.get(transactionId) ) {
        siteIdToSite.get(temp).removeTransactions(transactionId);
      }
      transactionIdToSites.remove(transactionId);
      if (flag) {
        runWaitList();
      }
    }
  }

  /**
   * run a single operation,
   * @param operation
   * @return true, this operation succeed and will be commited to each related site.
   *         false, this operation not succeed in commiting.
   */
  private boolean runOperation(Operation operation) {
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
      boolean flag = true;
      for (int i = 1; i <= 10; i++) {
        Site site = siteIdToSite.get(i);
        if (site.getSiteStatus() != Site.SiteStatus.FAIL && !site.commit(operation, transaction))  {
          flag = false;
        }
      }
      return flag;
    }
  }

  /**
   * fail this site and remove the transaction, which have write operation in this site and have not commited.
   * @param siteId
   */
  private void failSite(int siteId) {
    System.out.println();
    System.out.println("Failliing site " + siteId);
    Site site = siteIdToSite.get(siteId);
    List<Integer> transactionIdsOnTheSite = site.fail();
    if (!transactionIdsOnTheSite.isEmpty()) {
      System.out.println("Due to site fail, start aborting transactions in this site.");
      for (int transactionId : transactionIdsOnTheSite) {
        System.out.println("Abort transaction: T" + transactionId);
        abort(transactionIdToTransaction.get(transactionId));
      }
    }
    System.out.println("Site " + siteId + " failed");
    System.out.println();
  }

  /**
   * recove the site and run wailist of operration, some of which can be commited due to site recovery.
   * @param siteId
   */
  private void recoverSite(int siteId) {
    System.out.println();
    System.out.println("Recovering site " + siteId);
    Site site = siteIdToSite.get(siteId);
    site.recover();
    runWaitList();
    System.out.println("Site " + siteId + " recovered");
    System.out.println();
  }

  /**
   * dump all site with all variables
   */
  private void dump() {
    for (int i = 1; i <= 10; i++) {
      dump(i);
    }
  }

  /**
   * dump all site with a specific variable
   */
  private void dump(String variableId) {
    List<Integer> siteIds = variableIdToSiteId.get(Integer.parseInt(variableId));
    System.out.println();
    System.out.println("=== output of dump variable " + variableId);
    for (int siteId : siteIds) {
      Site site = siteIdToSite.get(siteId);
      System.out.print("Site" + siteId +" : ");
      site.dump(Integer.parseInt(variableId));
      System.out.println();
    }
  }

  /**
   * dump all variable in a specific site
   * @param siteId
   */
  private void dump(int siteId) {
    Site site = siteIdToSite.get(siteId);
    System.out.println();
    System.out.println("=== output of dump site " + siteId);
    site.dump();
    System.out.println();
  }

  /**
   * try to run read operation for every read including read and read-only.
   * And return the status of this operation.and run deadlock detection method to check whether it will
   * cause deadlock or not.
   * @param transactionId
   * @param variableId
   * @return true, read succeed and add lock for read in RW transaction
   *         false, fail to add lock for read in RW transaction.
   */
  public boolean readOperation(int transactionId, int variableId) {
    boolean res = true;
    if (!transactionIdToTransaction.containsKey(transactionId)) {
      System.err.println();
      System.err.println("Aborted transaction trying to perform read operation");
      return res;
    }
    Transaction transaction = transactionIdToTransaction.get(transactionId);
    // Add read operation of read only transaction
    if (transaction.getType() == Transaction.TranType.RO) {
      long operationTimestamp = transaction.getTimeStamp();
      // if odd indexed variable
      if (variableId % 2 == 1) {
        Site site = siteIdToSite.get(variableId % 10 + 1);
        Operation operation = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
        site.addOperation(transactionId, operation);
        Operation operation1 = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
        transactionIdToTransaction.get(transactionId).addOperations(operation1);
        site.commit(operation, transaction);
        System.out.print("\n");
      }
      // if even indexed variable
      else {
        List<Integer> siteIds = variableIdToSiteId.get(variableId);
        for (int siteId : siteIds) {
          Site site = siteIdToSite.get(siteId);
          Operation operation = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
          site.addOperation(transactionId, operation);
          if (site.commit(operation, transaction)) {
            break;
          }
        }
        Operation operation1 = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
        transactionIdToTransaction.get(transactionId).addOperations(operation1);
        System.out.print("\n");
      }
    }
    // Add read operation of read write transaction
    else {
      long operationTimestamp = System.nanoTime();
      // if odd indexed variable, only one site
      if (variableId % 2 == 1) {
        Site site = siteIdToSite.get(variableId % 10 + 1);
        Operation operation = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
        if (site.addLock(variableId, new Lock(variableId, transactionId, Lock.lockType.READ))) {
          site.addOperation(transactionId, operation);
          Operation operation1 = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
          transactionIdToTransaction.get(transactionId).addOperations(operation1);
          site.commit(operation, transaction);
          System.out.print("\n");
        }
        else {
          waitlistOperation.add(operation);
          res = false;
          // Add edge to graph by iterating lock table
          List<Lock> lockTable = site.getLockTable(variableId);
          for (Lock lock : lockTable) {
            if (lock.getType() == Lock.lockType.WRITE && lock.getTranscId() != transactionId) {
              graph.addNeighbor(lock.getTranscId(), transactionId);
            }
          }
          deadlockDetectAndAbort();
        }
      }
      // if even indexed variable, multiple site
      else {
        List<Integer> siteIds = variableIdToSiteId.get(variableId);
        boolean flag = false;
        Operation operation1 = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
        for (int siteId : siteIds) {
          Site site = siteIdToSite.get(siteId);
          Operation operation = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
          if (site.addLock(variableId, new Lock(variableId, transactionId, Lock.lockType.READ))) {
            site.addOperation(transactionId, operation);
            flag = true;
            site.commit(operation, transaction);
            break;
          }
        }
        if (!flag) {
          waitlistOperation.add(operation1);
          res = false;
          // Add edge to graph by iterating lock table
          Site site = siteIdToSite.get(1);
          for (int siteId : siteIdToSite.keySet()) {
            if (siteIdToSite.get(siteId).getSiteStatus() == Site.SiteStatus.NORMAL) {
              site = siteIdToSite.get(siteId);
              break;
            }
          }
          List<Lock> lockTable = site.getLockTable(variableId);
          for (Lock lock : lockTable) {
            if (lock.getType() == Lock.lockType.WRITE && lock.getTranscId() != transactionId) {
              graph.addNeighbor(lock.getTranscId(), transactionId);
            }
          }
          deadlockDetectAndAbort();
        } else {
          System.out.print("\n");
          Operation operation2 = new Operation(Operation.OpType.read, variableId, operationTimestamp, transactionId);
          transactionIdToTransaction.get(transactionId).addOperations(operation2);
        }
      }
    }
    return res;
  }

  /**
   * try to add write lock in related sites and run deadlock detection method to check whether it will
   * cause deadlock or not.
   * @param transactionId
   * @param variableId
   * @param newValue the write value for this variable
   * @return true, add lock succeed.
   *         false, can't acquire lock.
   */
  private boolean writeOperation(int transactionId, int variableId, int newValue) {
    boolean res = true;
    long operationTimestamp = System.nanoTime();
    // if odd indexed variable, only one site
    if (variableId % 2 == 1) {
      Operation operation = new Operation(Operation.OpType.write, variableId, newValue, operationTimestamp, transactionId);
      Site site = siteIdToSite.get(variableId % 10 + 1);
      if (site.addLock(variableId, new Lock(variableId, transactionId, Lock.lockType.WRITE))) {
        site.addOperation(transactionId, operation);
        Operation operation1 = new Operation(Operation.OpType.write, variableId, newValue, operationTimestamp, transactionId);
        transactionIdToTransaction.get(transactionId).addOperations(operation1);
      }
      else {
        waitlistOperation.add(operation);
        res = false;
        // Add edge to graph by iterating lock table
        List<Lock> lockTable = site.getLockTable(variableId);
        for (Lock lock : lockTable) {
          if (lock.getTranscId() != transactionId) {
            graph.addNeighbor(lock.getTranscId(), transactionId);
          }
        }
        deadlockDetectAndAbort();
      }

    }
    // if even indexed variable, multiple site
    else {
      List<Integer> siteIds = variableIdToSiteId.get(variableId);
      boolean flag = false;
      Operation operation1 = new Operation(Operation.OpType.write, variableId, newValue, operationTimestamp, transactionId);
      for (int siteId : siteIds) {
        Site site = siteIdToSite.get(siteId);
        Operation operation = new Operation(Operation.OpType.write, variableId, newValue, operationTimestamp, transactionId);
        if (site.getSiteStatus() == Site.SiteStatus.NORMAL) {
          if (!site.addLock(variableId, new Lock(variableId, transactionId, Lock.lockType.WRITE))) {
            break;
          }
          else {
            site.addOperation(transactionId, operation);
            flag = true;
          }
        }
      }
      if (flag) {
        for (int siteId : siteIds) {
          Operation operation = new Operation(Operation.OpType.write, variableId, newValue, operationTimestamp, transactionId);
          Site site = siteIdToSite.get(siteId);
          if (site.getSiteStatus() == Site.SiteStatus.RECOVERY) {
            site.addOperation(transactionId, operation);
          }
        }

        Operation operation2 = new Operation(Operation.OpType.write, variableId, newValue, operationTimestamp, transactionId);
        transactionIdToTransaction.get(transactionId).addOperations(operation2);
      }
      if (!flag) {
        waitlistOperation.add(operation1);
        res = false;
        // Add edge to graph by iterating lock table
        Site site = siteIdToSite.get(1);
        for (int siteId : siteIdToSite.keySet()) {
          if (siteIdToSite.get(siteId).getSiteStatus() == Site.SiteStatus.NORMAL) {
            site = siteIdToSite.get(siteId);
            break;
          }
        }
        List<Lock> lockTable = site.getLockTable(variableId);
        for (Lock lock : lockTable) {
          if (lock.getTranscId() != transactionId) {
            graph.addNeighbor(lock.getTranscId(), transactionId);
          }
        }
        deadlockDetectAndAbort();
      }


    }

    return res;
  }

  /**
   * detect whether it has deadlock in current status or not.
   * if has deadlock, it will abort the youngest transaction in the cycle of deadlock,
   * and release all the locks of this transaction in each related site.
   */
  private void deadlockDetectAndAbort() {
    List<Integer> deadLockTransactionIds = graph.detectDag();
    while (!deadLockTransactionIds.isEmpty()) {
      Transaction transaction = transactionIdToTransaction.get(deadLockTransactionIds.get(0));
      for (int i = 1; i < deadLockTransactionIds.size(); i++) {
        Transaction transaction1 = transactionIdToTransaction.get(deadLockTransactionIds.get(i));
        transaction = transaction.getTimeStamp() > (transaction1.getTimeStamp()) ? transaction : transaction1;
      }
      List<String> list = new ArrayList<>();
      for (Integer i : deadLockTransactionIds) {
        list.add("T" + i);
      }
      System.out.println("\nDeadlock detected: " + list.toString());
      System.out.println("Abort youngest transaction: T" + transaction.getTransactionId());
      System.out.println("T" + transaction.getTransactionId() +" aborted, due to deadlock.\n" );
      abort(transaction);
      deadLockTransactionIds = graph.detectDag();
    }
  }

  /**
   * abort a transaction, release its locks in each related site;
   * and remove it from map and related vertex from the graph.
   * @param transaction
   */
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
    transactionIdToSites.remove(transaction.getTransactionId());
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
    runWaitList();
  }

  /**
   * try to run the waiting (blocked) transaction's operation, and acquire lock if needed.
   */
  private void runWaitList() {
    for (Operation operation : waitlistOperation) {
      int transId = operation.getTransId();
      Transaction transaction = transactionIdToTransaction.get(transId);
      waitlistOperation.remove(operation);

      if (operation.getType() == Operation.OpType.read) {
        if (readOperation(operation.getTransId(), operation.getVariableIndex())) {

          System.out.println("\nWaiting Operation finished: " + " T" + transId + " " + operation.getType().toString() + " x"
                  + operation.getVariableIndex() + "\n");
          if (transaction.getOperations().size() != 0)
            continue;
          System.out.println("Waiting Operation starts to commit.");
          endTransaction(operation.getTransId());
          System.out.println("Waiting Operation commited.");
        }
      }
      else {
        if (writeOperation(operation.getTransId(), operation.getVariableIndex(), operation.getValue())) {

          System.out.println("\nWaiting Op got lock:" + " T" + transId + " " + operation.getType().toString() + " x"
                  + operation.getVariableIndex() + " " +  "with " +operation.getValue() + "\n");
          if (transaction.getOperations().size() != 0)
            continue;
          System.out.println("Waiting Operation starts to commit.");
          endTransaction(operation.getTransId());
          System.out.println("Waiting Operation commited.");
        }

      }
    }
  }

}
