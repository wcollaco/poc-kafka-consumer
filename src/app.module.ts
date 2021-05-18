import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { ScheduleModule } from '@nestjs/schedule';
import { CronjobService } from './cronjob/cronjob.service';

@Module({
  imports:  [
    ScheduleModule.forRoot()
    ],
  controllers: [AppController],
  providers: [AppService, CronjobService],
})
export class AppModule {}
