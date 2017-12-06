package edu.nyu.adb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class TransactionManager {

    private File file;
    private BufferedReader bufferedReader;
    private HashMap<Integer, List<Integer>> variableToSiteId;
    private HashMap<Integer, Site> siteIdToSite;
    private HashMap<Integer, Transaction> transactionIdToTransaction;


    public TransactionManager(String inputFilePath) {
        variableToSiteId = new HashMap<>();
        for (int i = 1; i <= 20; i++) {
            variableToSiteId.putIfAbsent(i, new ArrayList<>());
            if (i % 2 == 0) {
                for (int j = 1; j <= 10; j++) {
                    variableToSiteId.get(i).add(j);
                }
            }
            else {
                variableToSiteId.get(i).add(i % 10 + 1);
            }
        }
        siteIdToSite = new HashMap<>();
        for (int i = 1; i <= 10; i++) {
            siteIdToSite.put(i, new Site(i));
        }
        transactionIdToTransaction = new HashMap<>();
        try {
            file = new File(inputFilePath);
            bufferedReader = new BufferedReader(new FileReader(file));
        }
        catch (Exception e) {
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
        }
        catch (Exception e) {
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
            endTransaction(Integer.parseInt(transactionId));
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
            readOperation(Integer.parseInt(transactionId), Integer.parseInt(variableId));
        }
        else if (command.startsWith("W")) {
            String[] parameters = command.substring(command.indexOf("(") + 1, command.indexOf(")")).split(",");
            String transactionId = parameters[0].substring(1);
            String variableId = parameters[1].substring(1);
            String newValue = parameters[2];
            writeOperation(Integer.parseInt(transactionId), Integer.parseInt(variableId), Integer.parseInt(newValue));
        }
        else {
            System.err.println("Error input file format.");
        }
    }

    private void startTransaction(String type, int transactionId) {
        if (type.equals("RW")) {
            transactionIdToTransaction.put(transactionId, new Transaction(transactionId, new Date(), Transaction.TranType.RW));
        }
        else {
            // TODO: read in all variables for further read in this ReadOnly transaction
            transactionIdToTransaction.put(transactionId, new Transaction(transactionId, new Date(), Transaction.TranType.RO));
        }
    }

    private void endTransaction(int transactionId) {
        //TODO: end transaction
    }

    private void failSite(int siteId) {
        // TODO: fail
        System.out.println();
        System.out.println("Failliing site " + siteId);
        Site site = siteIdToSite.get(siteId);
        List<Integer> transactionIdsOnTheSite = site.fail();
        for (int siteIdForAbortTransaction : siteIdToSite.keySet()) {
            Site siteForAbortTransaction = siteIdToSite.get(siteIdForAbortTransaction);
            for (int transactionId : transactionIdsOnTheSite) {
                if (siteForAbortTransaction.removeTransactions(transactionId)) {
                    System.out.println("Aborting transaction " + transactionId + " at site " + siteIdForAbortTransaction)
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
        List<Integer> siteIds = variableToSiteId.get(variableId);
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



}
