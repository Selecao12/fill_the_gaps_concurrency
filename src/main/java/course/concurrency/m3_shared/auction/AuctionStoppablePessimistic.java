package course.concurrency.m3_shared.auction;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AuctionStoppablePessimistic implements AuctionStoppable {

    private final ReentrantReadWriteLock lock;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;

    private Notifier notifier;

    public AuctionStoppablePessimistic(Notifier notifier) {
        this.notifier = notifier;
        lock = new ReentrantReadWriteLock();
        readLock = lock.readLock();
        writeLock = lock.writeLock();
    }

    private volatile Bid latestBid;
    private volatile boolean auctionIsClosed;

    public boolean propose(Bid bid) {
        try {
            writeLock.lock();
            if (auctionIsClosed) {
                return false;
            }
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

    public Bid stopAuction() {
        try {
            writeLock.lock();
            auctionIsClosed = true;
            return latestBid;
        } finally {
            writeLock.unlock();
        }
    }
}
