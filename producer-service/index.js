const express = require("express");
const { Kafka } = require("kafkajs");

const app = express();
app.use(express.json());

const kafka = new Kafka({
    clientId: "order-service",
    brokers: ["localhost:9092"]
});

const producer = kafka.producer();

(async () => {
    await producer.connect();
})();

app.post("/orders", async (req, res) => {
    const order = {
        orderId: Date.now(),
        customer: req.body.customer,
        amount: req.body.amount
    };

    await producer.send({
        topic: "orders",
        messages: [
            {
                key: order.orderId.toString(),
                value: JSON.stringify(order)
            }
        ]
    });

    res.json({
        message: "Order created",
        order
    });
});

app.listen(3000, () => {
    console.log("Order Service running on port 3000");
});
