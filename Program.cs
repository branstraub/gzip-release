using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;


namespace gzip
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var options = new Options();
            options.ConnectionStringSource = args[0];
            options.ConnectionStringDestination = args[1];
            string queueName = args[2];
            string queueNameLog = args[4];
            string queueCS = args[3];
            var storageAccount = CloudStorageAccount.Parse(args[0]);
            var queueClient = storageAccount.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(queueName);

            var stopWatch = new Stopwatch();
            stopWatch.Start();
            var msg = ".";

            while(msg != "null"){
            
                var retrievedMessage = await queue.GetMessageAsync();

                if(retrievedMessage != null){
                    msg = retrievedMessage.AsString;
                }else{
                    msg = "null";
                    return;
                }
            
                var container = msg.Split('/')[0];
                var prefix = msg.Substring(msg.IndexOf('/')+1);
                Console.WriteLine(container+" "+prefix);
               
                var storageAccountS = CloudStorageAccount.Parse(options.ConnectionStringSource);
                var storageAccountD = CloudStorageAccount.Parse(options.ConnectionStringDestination);
                
                var blobClientS = storageAccountS.CreateCloudBlobClient();
                var blobContainerS = blobClientS.GetContainerReference(container);

                var blobClientD = storageAccountD.CreateCloudBlobClient();
                var blobContainerD = blobClientD.GetContainerReference("gzip-"+container);

                // Do the compression work
                await new Utility().EnsureGzipFiles(blobContainerS, blobContainerD, prefix, queueCS,queueName, queueNameLog);

                await queue.DeleteMessageAsync(retrievedMessage);
                //await Task.Delay(500);
            }

            stopWatch.Stop();
            var ts = stopWatch.Elapsed;

            var queueLog = queueClient.GetQueueReference(queueNameLog);
            var message = new CloudQueueMessage("RunTime " + ts);
            await queueLog.AddMessageAsync(message);
        }
    }
}
    