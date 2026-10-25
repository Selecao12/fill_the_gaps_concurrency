package course.concurrency.m3_shared.immutable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class OrderService {

    private final ConcurrentHashMap<Long, Order> currentOrders = new ConcurrentHashMap<>();

    private final AtomicLong nextId = new AtomicLong(0);

    private long nextId() {
        return nextId.incrementAndGet();
    }

    public long createOrder(List<Item> items) {
        long id = nextId();
        Order order = new Order(id, items);
        currentOrders.put(id, order);
        return id;
    }

    public synchronized void updatePaymentInfo(long orderId, PaymentInfo paymentInfo) {
        currentOrders.computeIfPresent(orderId, (id, order) -> {
            var value = order.withPaymentInfo(paymentInfo);
            if (value.checkStatus()) {
                return value.withDeliveredStatus();
            }
            return value;
        });
    }

    public synchronized void setPacked(long orderId) {
        currentOrders.computeIfPresent(orderId, (id, order) -> {
            var value = order.withPacked();
            if (value.checkStatus()) {
                return value.withDeliveredStatus();
            }
            return value;
        });
    }

    public boolean isDelivered(long orderId) {
        return currentOrders.get(orderId).getStatus().equals(Order.Status.DELIVERED);
    }
}
