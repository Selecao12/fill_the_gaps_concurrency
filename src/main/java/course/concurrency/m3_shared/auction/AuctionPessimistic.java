package course.concurrency.m3_shared.auction;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AuctionPessimistic implements Auction {

    private final ReentrantReadWriteLock lock;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;

    private Notifier notifier;

    public AuctionPessimistic(Notifier notifier) {
        this.notifier = notifier;
        lock = new ReentrantReadWriteLock();
        readLock = lock.readLock();
        writeLock = lock.writeLock();
    }

    private volatile Bid latestBid;

    public boolean propose(Bid bid) {
        try {
            writeLock.lock();
            if (latestBid == null) {
                latestBid = bid;
                return true;
            }
            if (bid.getPrice() > latestBid.getPrice()) {
                notifier.sendOutdatedMessage(latestBid);
                latestBid = bid;
                return true;
            }
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    public Bid getLatestBid() {
        try {
            readLock.lock();
            return latestBid;
        } finally {
            readLock.unlock();
        }
    }
}
