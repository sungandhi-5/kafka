const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "payment-service",
    brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({
    groupId: "payment-group"
});

const DLQProducer = kafka.producer();

async function consume() {
    await consumer.connect();

    await consumer.subscribe({
        topic: "orders",
        fromBeginning: true
    });

    await consumer.run({
        eachMessage: async ({ message }) => {

            const order = JSON.parse(message.value.toString());
            try {
    
                console.log("Received Order:", order);
    
                console.log("Processing payment...");
    
                await new Promise(resolve => setTimeout(resolve, 2000));
    
                if(order.amount > 1000){
                    throw new Error("Payment gateway timeout");
                }
    
                console.log(`Payment successful for Order ${order.orderId}`);
            } catch (error) {
                console.error(error);
                moveToDLQ(order,error)
            }
        }
    });
}

async function producer(){
    await DLQProducer.connect();
}

async function moveToDLQ(order, error) {

    await DLQProducer.send({
        topic: "orders-dlq",
        messages: [
            {
                key: order.orderId.toString(),
                value: JSON.stringify({
                    order,
                    error: error.message,
                    failedAt: new Date().toISOString()
                })
            }
        ]
    });

    console.log("Moved to DLQ");
}

producer();
consume();
