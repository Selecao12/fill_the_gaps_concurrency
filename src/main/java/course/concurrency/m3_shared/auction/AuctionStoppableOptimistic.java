package course.concurrency.m3_shared.auction;

import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;

public class AuctionStoppableOptimistic implements AuctionStoppable {

    private Notifier notifier;

    public AuctionStoppableOptimistic(Notifier notifier) {
        this.notifier = notifier;
    }

    private final AtomicMarkableReference<Bid> latestBidReference = new AtomicMarkableReference<>(null, false);

    public boolean propose(Bid bid) {
        Bid latestBid;
        do {
            if (latestBidReference.isMarked()) {
                return false;
            }
            latestBid = latestBidReference.getReference();
            if (latestBid != null && bid.getPrice() <= latestBid.getPrice()) {
                return false;
            }
        } while (!latestBidReference.compareAndSet(latestBid, bid, false, false));

        notifier.sendOutdatedMessage(latestBid);
        return true;
    }

    public Bid getLatestBid() {
        return latestBidReference.getReference();
    }

    public Bid stopAuction() {
        Bid latestBid;
        do {
            latestBid = latestBidReference.getReference();
        } while (!latestBidReference.attemptMark(latestBid, true));
        return latestBid;
    }
}
