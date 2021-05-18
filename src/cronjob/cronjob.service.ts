import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { Producer, KafkaConsumer, AdminClient, TopicPartitionOffset, TopicPartition } from 'node-rdkafka';
import { isArray } from 'util';

@Injectable()
export class CronjobService {
    private Consumer: KafkaConsumer;
    private readonly logger = new Logger(CronjobService.name);      
    private executing = false;
    private assignAttempt = 0;
    private currentPartitionsAndOffsets = [];
    private regCount = 0;
    private isDisconnecting = false;

    updateAssignments(arr)
    {
        try{          
          var partitionsAssigned = this.Consumer.assignments();    
        }
        catch(ex){
          console.log("Consumer broke.");
          return arr;
        }
      
      var filteredArray = [];      
      for(var i = 0; i < partitionsAssigned.length; i++)
        {
          var currentElement = arr.find(x => x["partition"] === partitionsAssigned[i].partition);

          if(typeof currentElement === "undefined")
          {
              console.log("New partition!" + partitionsAssigned[i].partition);
              var wateroff = this.Consumer.getWatermarkOffsets('user-interaction', partitionsAssigned[i].partition);
              filteredArray.push({"partition": partitionsAssigned[i].partition, "lastOffset:": wateroff.highOffset - 1, "paused": false});

          } else {
              filteredArray.push({"partition": currentElement["partition"], "lastOffset:": currentElement["lastOffset"], "paused": currentElement["paused"]});
          }
        }
        console.log("Updated assignments. Current state: ");
        console.log(JSON.stringify(filteredArray));

        return filteredArray;
    }

    isAllPartitionsPaused(arr) {
      if(typeof arr === "undefined")
      {
        return false;
      }

      var allPaused = arr.every((x) => x["paused"] == true);      
      
      return allPaused;
    }

    markAsPaused(arr, partition) {
      for(var i = 0; i < arr.length; i++)
      {
        if(arr[i]["partition"] == partition)
        {
          arr[i]["paused"] = true;
          console.log(`Partition ${arr[i]["partition"]} marked as paused.`);
        }
      }
      return arr;
    }


    @Cron('* * * * *')
    async handleCron() {
      // if(this.executing)
      // {
      //   return;
      // }
      // this.executing=true;
      this.regCount = 0;
      this.isDisconnecting = false;
      this.currentPartitionsAndOffsets = [];
      this.Consumer = new KafkaConsumer({
        'group.id': 'poc-kafka-consumer' + 1,
        'bootstrap.servers': 'localhost:9092',
        'debug': 'all',
        'enable.partition.eof': true
      }, {}); 

      
      await this.Consumer.connect();

      var self = this;
      
      try {         
          this.Consumer.on('ready', async function() {
            await self.Consumer.subscribe(['user-interaction']);            

            var isFullAssignDone = false;

            while(!isFullAssignDone) {              
              var topicsAssigned = self.Consumer.assignments();
              
              if(topicsAssigned.length == 0) {
                console.log("Still no partition assigned...");       
                self.assignAttempt++;  
                console.log(self.assignAttempt);
                continue;
              }

              var wateroff = self.Consumer.getWatermarkOffsets('user-interaction', topicsAssigned[0].partition);
              if(wateroff.highOffset < 0 && wateroff.lowOffset < 0)
              {
                  console.log("Still no watermarks.")
                  continue;
              }
              
              console.log("Its alive!");

              topicsAssigned.forEach((x) => {                
                var waterMarksForTopic = self.Consumer.getWatermarkOffsets('user-interaction', x.partition);
                  self.currentPartitionsAndOffsets.push({"partition": x.partition, "lastOffset:": waterMarksForTopic.highOffset - 1, "paused": false})
                console.log(`Offsets loaded for partition ${x.partition}. HighOffset: ${waterMarksForTopic.highOffset}`);
              });

              isFullAssignDone=true;               
            }

            self.Consumer.consume();           
          })
          .on('data', function(data) {
            var currentTopicOffset = self.currentPartitionsAndOffsets.filter(element => element["partition"] == data.partition);
            console.log(JSON.stringify(self.currentPartitionsAndOffsets));

            if(currentTopicOffset["lastOffset"] == data.offset) {                            
              self.Consumer.pause([{partition: data.partition, topic: data.topic}]);  
              console.log(`Partition ${data.partition} paused. Reached the end of offsets 'start state'.`);

              self.currentPartitionsAndOffsets = self.markAsPaused(self.currentPartitionsAndOffsets, data.partition);

              self.currentPartitionsAndOffsets = self.updateAssignments(self.currentPartitionsAndOffsets);
            
              var shouldDisconnectAfterData = self.isAllPartitionsPaused(self.currentPartitionsAndOffsets);              
            }

              /********* 
               * ***** *
               * STUFF *
               * ***** *
               *********/

            self.regCount++;                      
            console.log(`MSG P:${data.partition}: ${data.value.toString()}`);
            
            if(shouldDisconnectAfterData)
            {
              self.Consumer.disconnect();
            }
          }).on('partition.eof', function(part) {
            if(self.isDisconnecting){
              return;
            }
            if(self.currentPartitionsAndOffsets.some(x=> x.partition == part.partition && x.pause == true) || !self.currentPartitionsAndOffsets.some(x=> x.partition == part.partition))
            {
              console.log(`Partition ${part.partition} is paused or not being managed by this instance.`);
              return;
            }
            
            self.currentPartitionsAndOffsets = self.updateAssignments(self.currentPartitionsAndOffsets);

            console.log(`Partition ${part.partition} EOF`);

            self.currentPartitionsAndOffsets = self.markAsPaused(self.currentPartitionsAndOffsets, part.partition);
            self.Consumer.pause([{partition: part.partition, topic: part.topic}]);              

            if(self.isAllPartitionsPaused(self.currentPartitionsAndOffsets))
            {
              console.log("Disconnecting...")
              self.isDisconnecting = true;
              self.Consumer.disconnect();
            }

          }).on('disconnected', function() {
            console.log(`Consumer disconnected. ${self.regCount} messages delivered.`);
          });
        } 
        catch(err) {
          console.log(err);
        }
    }
  }