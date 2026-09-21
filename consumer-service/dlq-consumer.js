const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "payment-service",
    brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({
    groupId: "DLQ-payment-group"
});

async function consume() {
    await consumer.connect();

    await consumer.subscribe({
        topic: "orders-dlq",
        fromBeginning: true
    });

    await consumer.run({
        eachMessage: async ({ message }) => {

            const order = JSON.parse(message.value.toString());
            try {
    
                console.log("Received Order In DLQ :", order);
    
                console.log("Processing payment...");
    
                console.log(`Payment successful for Order ${order.orderId}`);
            } catch (error) {
                console.error(error);
                moveToDLQ(order,error)
            }
        }
    });
}

consume();
