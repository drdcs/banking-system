package com.distributed;

import java.io.InputStream;
import java.util.*;

public class IncomingTransactionsReader implements Iterator<Transactions> {

    private static final String USER_TRANSACTION_STATIC_FILE = "user-transactions.txt";
    private final List<Transactions> transactions;
    private final Iterator<Transactions> transactionsIterator;

    public IncomingTransactionsReader() {
        this.transactions = loadTransactions();
        this.transactionsIterator = transactions.iterator();
    }

    private List<Transactions> loadTransactions() {

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(USER_TRANSACTION_STATIC_FILE);
        Scanner scanner = new Scanner(inputStream);
        List<Transactions> transactions = new ArrayList<>();

        while(scanner.hasNextLine()) {
            String[] transaction = scanner.nextLine().split(" ");
            String user = transaction[0];
            String transactionLocation = transaction[1];
            double amount = Double.valueOf(transaction[2]);
            transactions.add(new Transactions(user, amount, transactionLocation));
        }
        return Collections.unmodifiableList(transactions);
    }


    @Override
    public boolean hasNext() {
        return transactionsIterator.hasNext();
    }

    @Override
    public Transactions next() {
        return transactionsIterator.next();
    }
}
