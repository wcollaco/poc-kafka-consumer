import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ConsumerController } from './consumer/consumer.controller';

@Module({
  imports: [],
  controllers: [AppController, ConsumerController],
  providers: [AppService],
})
export class AppModule {}
