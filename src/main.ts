import { NestFactory } from '@nestjs/core';
import { Transport } from '@nestjs/microservices'
import { AppModule } from './app.module';
import { ConsumerController } from './consumer/consumer.controller';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { cors:true });
  
  app.connectMicroservice({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'poc' + 1,
        brokers: ['host.docker.internal:9094'],
      },
      consumer: {
        groupId: 'poc-kafka-consumer' + 1,
        autoCommit: false
      }
    }
  })
  await app.startAllMicroservicesAsync();
  new ConsumerController().startListener();
}
bootstrap();
