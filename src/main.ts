import { NestFactory } from '@nestjs/core';
import { Transport } from '@nestjs/microservices'
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { cors:true });
  
  await app.startAllMicroservicesAsync();
  await app.listen(3015);
}
bootstrap();
