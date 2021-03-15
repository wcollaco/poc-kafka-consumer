import {
    SchemaRegistry,
  } from '@kafkajs/confluent-schema-registry';
  import { Kafka } from 'kafkajs';

  
export class ConsumerController {

    startListener(){
        const kafka = new Kafka({
            clientId: 'poc' + 1,
            brokers: ['host.docker.internal:9094']
          });
          
          const registry = new SchemaRegistry({
            host: 'http://host.docker.internal:8081'
          });
                    
          const consumer = kafka.consumer({ groupId: 'poc-kafka-consumer' + 1 });
          (async () => {
            await consumer.connect();
          })().catch(console.error);
          
          // Consume messages
          (async () => {
            await consumer.subscribe({
              topic: 'user-interaction',
              fromBeginning: true
            });
          
            await consumer.run({
              eachMessage: async ({ topic, partition, message }: any) => {
                console.log(
                  `Message received from topic '${topic}'[${partition}|${message.offset}]`
                );
          
                // Try to decode the message (if it's not an avro it will fail and go to the catch statement)
                try {
                  const decodedPayload = await registry.decode(message.value);
                  console.log(decodedPayload);
                } catch (e) {
                  console.log(message.value.toString()); // Print the pure message without decoding
                }
              }
            });
          })().catch(console.error);
    }
}
