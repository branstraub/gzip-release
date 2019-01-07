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
            options.BlobConnectionStringSource = args[0];
            options.BlobConnectionStringDestination = args[1];
            options.QueueConnectionString = args[2];
            options.QueueName = args[3];
          
            var storageAccount = CloudStorageAccount.Parse(options.BlobConnectionStringSource);
            var queueClient = storageAccount.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(options.QueueName);

            var stopWatch = new Stopwatch();
            stopWatch.Start();
            var msg = ".";
            
            //msg = "https://gzipo.blob.core.windows.net/gzipi/2018/april/0/ac583c70-df76-4e02-a70f-7a3d06a0985d.json";

            while(msg != "null"){
            
                var retrievedMessage = await queue.GetMessageAsync();
                await queue.DeleteMessageAsync(retrievedMessage);

                if(retrievedMessage != null){
                    msg = retrievedMessage.AsString;
                }else{
                    msg = "null";
                    return;
                }
            
                var container = msg.Split('/')[3];
                var fileName = msg.Split('/').Last();
               
                var storageAccountS = CloudStorageAccount.Parse(options.BlobConnectionStringSource);
                var storageAccountD = CloudStorageAccount.Parse(options.BlobConnectionStringDestination);
                
                var blobClientS = storageAccountS.CreateCloudBlobClient();
                var blobContainerS = blobClientS.GetContainerReference(container);

                var blobClientD = storageAccountD.CreateCloudBlobClient();
                var blobContainerD = blobClientD.GetContainerReference("gzip-"+container);

                // Do the compression work
                await new Utility().EnsureGzipFiles(blobContainerS, blobContainerD, fileName);

            }

            stopWatch.Stop();
            var ts = stopWatch.Elapsed;

        }
    }
}
    