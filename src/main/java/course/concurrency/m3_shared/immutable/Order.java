package course.concurrency.m3_shared.immutable;

import java.util.Collections;
import java.util.List;

import static course.concurrency.m3_shared.immutable.Order.Status.NEW;

public class Order {

    public enum Status {NEW, IN_PROGRESS, DELIVERED}

    private final Long id;
    private final List<Item> items;
    private final PaymentInfo paymentInfo;
    private final boolean isPacked;
    private final Status status;

    public Order(long id, List<Item> items) {
        this.id = id;
        this.items = items.stream().toList();
        this.status = NEW;
        this.isPacked = false;
        this.paymentInfo = null;
    }

    private Order(Order order, PaymentInfo paymentInfo) {
        this.id = order.id;
        this.items = order.items;
        this.paymentInfo = paymentInfo;
        this.isPacked = order.isPacked;
        this.status = Status.IN_PROGRESS;
    }

    private Order(Order order, boolean isPacked) {
        this.id = order.id;
        this.items = order.items;
        this.paymentInfo = order.paymentInfo;
        this.isPacked = isPacked;
        this.status = Status.IN_PROGRESS;
    }

    private Order(Order order, Status status) {
        this.id = order.id;
        this.items = order.items;
        this.paymentInfo = order.paymentInfo;
        this.isPacked = order.isPacked;
        this.status = status;
    }

    public boolean checkStatus() {
        return items != null && !items.isEmpty() && paymentInfo != null && isPacked;
    }

    public Long getId() {
        return id;
    }

    public List<Item> getItems() {
        return Collections.unmodifiableList(items);
    }

    public PaymentInfo getPaymentInfo() {
        return paymentInfo;
    }

    public Order withPaymentInfo(PaymentInfo paymentInfo) {
        return new Order(this, paymentInfo);
    }

    public boolean isPacked() {
        return isPacked;
    }

    public Order withPacked() {
        return new Order(this, true);
    }

    public Status getStatus() {
        return status;
    }

    public Order withDeliveredStatus() {
        return new Order(this, Status.DELIVERED);
    }
}
