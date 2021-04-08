import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import {
    SchemaRegistry,
  } from '@kafkajs/confluent-schema-registry';
  import { Kafka, Consumer } from 'kafkajs';
  import { Controller, Get, Inject, OnModuleInit } from '@nestjs/common';
  import { ClientKafka  } from '@nestjs/microservices';

@Injectable()
export class CronjobService {
    private consumer: Consumer;
    private readonly logger = new Logger(CronjobService.name);    
    constructor(
      @Inject('KAFKA_SERVICE')
      private readonly kafka: ClientKafka,
  ) {}
  
    @Cron('* * * * *')
    async handleCron() {
      let fetchCount = 0;
      //Dados de conexão com o Kafka
      const kafka = new Kafka({
          clientId: 'poc' + 1,
          brokers: ['host.docker.internal:9094']
        });
  
        //Conexão com o kafka.admin. Para recuperar metadados dos tópicos.
        const admin = kafka.admin()
          
        await admin.connect()
  
        //Recupera os offsets do tópico.
        var lastOffsets = await admin.fetchTopicOffsets('user-interaction');
        console.log(lastOffsets);
  
        // garantir a desconexão.
        await admin.disconnect()
              
        //Não vamos utilizar
        const registry = new SchemaRegistry({
          host: 'http://host.docker.internal:8081'
        });
                  
        //Cria o consumer
        this.consumer = kafka.consumer({ groupId: 'poc-kafka-consumer' + 1 });
  
        (async () => {
          await this.consumer.connect();
        })().catch(console.error);
        
        // Consume messages
        (async () => {
          await this.consumer.subscribe({
            topic: 'user-interaction',
            fromBeginning: true
          });
          let assignedPartitions = 0;
          
          const { GROUP_JOIN, START_BATCH_PROCESS, FETCH } = this.consumer.events
  
          this.consumer.on(GROUP_JOIN, async event => {            
            assignedPartitions = event.payload.memberAssignment
          });

          this.consumer.on(START_BATCH_PROCESS, async event => {
            fetchCount = 0;
          });

          this.consumer.on(FETCH, async event => {
            fetchCount++;

            if(fetchCount == 3) {
                this.consumer.disconnect();
                this.kafka.close();
            }
          });
          await this.consumer.run({          
            eachMessage: async ({ topic, partition, message }: any) => { 
              try {
  
                const decodedPayload = await registry.decode(message.value);
  
                this.checkIfShouldPause(lastOffsets, partition, message.offset);
                   
              } catch (e) {
                console.log(message.value.toString()); // Print the pure message without decoding
              }
            }
          });                  
        })().catch(console.error);
    console.log("END OF CRONJOB");
    }
    
    async checkIfShouldPause(lastOffsets, partition, currentOffset) {
      lastOffsets.forEach(element => {
        if((Number(element.partition)) == partition && (Number(element.offset) -1) == currentOffset)
        {
           this.consumer.pause([{'topic':'user-interaction', 'partitions':[partition]}]);
           console.log(`Partition ${partition} Paused`);
           console.log(JSON.stringify(this.consumer.paused()));
        }          
       })
       if(this.consumer.paused().length > 0 && this.consumer.paused()[0].partitions.length == 3)
       {
         console.log("Disconnecting...");
         await this.consumer.disconnect();
         await this.kafka.close();
         console.log("Disconnected.");
       }
      }
  }