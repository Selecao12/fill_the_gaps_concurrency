package course.concurrency.m2_async.minPrice;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.*;

public class PriceAggregator {

    private PriceRetriever priceRetriever = new PriceRetriever();

    private Executor executor = Executors.newFixedThreadPool(130);

    public void setPriceRetriever(PriceRetriever priceRetriever) {
        this.priceRetriever = priceRetriever;
    }

    private Collection<Long> shopIds = Set.of(10l, 45l, 66l, 345l, 234l, 333l, 67l, 123l, 768l);

    public void setShops(Collection<Long> shopIds) {
        this.shopIds = shopIds;
    }

    public double getMinPrice(long itemId) {
        var completableFutures = shopIds.stream().map(shopId ->
                CompletableFuture.supplyAsync(() -> priceRetriever.getPrice(itemId, shopId), executor)
        ).toList();

        try {
            CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]))
                    .get(2800, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException | TimeoutException e) {
        }

        return completableFutures.stream()
                .filter(CompletableFuture::isDone)
                .filter(cf -> !cf.isCompletedExceptionally())
                .mapToDouble(CompletableFuture::join)
                .min()
                .orElse(Double.NaN);
    }
}
