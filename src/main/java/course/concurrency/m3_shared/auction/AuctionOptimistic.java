package course.concurrency.m3_shared.auction;

import java.util.concurrent.atomic.AtomicReference;

public class AuctionOptimistic implements Auction {

    private Notifier notifier;

    public AuctionOptimistic(Notifier notifier) {
        this.notifier = notifier;
    }

    private final AtomicReference<Bid> latestBidReference = new AtomicReference<>();

    public boolean propose(Bid bid) {
        Bid latestBid;
        do {
            latestBid = latestBidReference.get();
            if (latestBid != null && bid.getPrice() <= latestBid.getPrice()) {
                return false;
            }
        } while (!latestBidReference.compareAndSet(latestBid, bid));

        notifier.sendOutdatedMessage(latestBid);
        return true;
    }

    public Bid getLatestBid() {
        return latestBidReference.get();
    }
}
