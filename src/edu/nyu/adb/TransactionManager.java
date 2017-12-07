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
      String transactionId = command.substring(command.indexOf("T") + 1, command.indexOf(")"));
      if (transactionIdToTransaction.containsKey(transactionId)) {
        endTransaction(Integer.parseInt(transactionId));
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
      String transactionId = command.substring(command.indexOf("T") + 1, command.indexOf(","));
      String variableId = command.substring(command.indexOf("x") + 1, command.indexOf(")"));
      if (transactionIdToTransaction.containsKey(transactionId)) {
        addSiteIdToTransactionMap(Integer.parseInt(transactionId), Integer.parseInt(variableId));
        readOperation(Integer.parseInt(transactionId), Integer.parseInt(variableId));
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
    }
    else {
      transactionIdToTransaction.put(transactionId, new Transaction(transactionId, new Date(), Transaction.TranType.RO));
    }
    System.out.println();
  }

  private void endTransaction(int transactionId) {
    Transaction transaction = transactionIdToTransaction.get(transactionId);
    if (transaction.getType() == Transaction.TranType.RO) {
      List<Operation> operationList = transaction.getOperations();
      for (Operation operation : operationList) {
        int variableId = operation.getVariableIndex();
        if (variableId % 2 == 1) {
          Site site = siteIdToSite.get(variableId % 2 + 1);
          if (site.commit(operation, transaction)) {
            continue;
          }
          else {
            waitlistOperation.add(operation);
          }
        }
        else {
          boolean flag = true;
          for (int i = 1; i <= 10; i++) {
            Site site = siteIdToSite.get(i);
            if (site.commit(operation, transaction)) {
              flag = false;
              break;
            }
          }
          if (flag) {
            waitlistOperation.add(operation);
          }
        }
      }
    }
    
  }

  private void failSite(int siteId) {
    System.out.println();
    System.out.println("Failliing site " + siteId);
    Site site = siteIdToSite.get(siteId);
    List<Integer> transactionIdsOnTheSite = site.fail();
    for (int transactionId : transactionIdsOnTheSite) {
      transactionIdToTransaction.remove(transactionId);
    }
    for (int siteIdForAbortTransaction : siteIdToSite.keySet()) {
      Site siteForAbortTransaction = siteIdToSite.get(siteIdForAbortTransaction);
      for (int transactionId : transactionIdsOnTheSite) {
        if (siteForAbortTransaction.removeTransactions(transactionId)) {
          System.out.println("Aborting transaction " + transactionId + " at site " + siteIdForAbortTransaction);
        }
      }
    }
    siteIdToSite.remove(siteId);
    System.out.println("Site " + siteId + " failed");
    System.out.println();
  }

  private void recoverSite(int siteId) {
    System.out.println();
    System.out.println("Recovering site " + siteId);
    Site site = siteIdToSite.get(siteId);
    site.recover();
    System.out.println("Site " + siteId + " recovered");
    System.out.println();
  }

  private void dump() {
    for (int i = 1; i <= 10; i++) {
      dump(i);
    }
  }

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
    Date operationTimestamp = transaction.getTimeStamp();
    if (transaction.getType() == Transaction.TranType.RO) {
      if (variableId % 2 == 1) {
        Site site = siteIdToSite.get(variableId % 10 + 1);
        Operation operation = new Operation(Operation.OpType.read, variableId, operationTimestamp);
        site.addOperation(transactionId, operation);
      } else {
        List<Integer> siteIds = variableIdToSiteId.get(variableId);
        for (int siteId : siteIds) {
          Site site = siteIdToSite.get(siteId);
          Operation operation = new Operation(Operation.OpType.read, variableId, operationTimestamp);
          site.addOperation(transactionId, operation);
        }
      }
    } else {

    }
  }

  public void writeOperation(int transactionId, int variableId, int newValue) {
    // TODO: write operation
  }

}
