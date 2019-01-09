using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Shared.Protocol;
using Microsoft.WindowsAzure.Storage.Queue;

namespace gzip
{
    class Utility
    {
       public async Task EnsureGzipFiles(CloudBlobContainer containerS, CloudBlobContainer containerD, string fileName, string queueName, string queueString, string originalMessage)
        {
          
            var blobInfo = containerS.GetBlobReference(fileName);
            await Upload(blobInfo, containerD, queueName, queueString, originalMessage);
            
        }

        public async Task Upload(IListBlobItem blobInfo, CloudBlobContainer containerD, string queueName, string queueString, string originalMessage){
            
                var blob = (CloudBlob)blobInfo; 
                byte[] compressedBytes;

                using (var memoryStream = new MemoryStream())
                {
                    using (var gzipStream = new GZipStream(memoryStream, CompressionMode.Compress))
                    using (var blobStream = await blob.OpenReadAsync())
                    {
                        await blobStream.CopyToAsync(gzipStream);
                      
                    }
                    compressedBytes = memoryStream.ToArray();
                }

                containerD.CreateIfNotExistsAsync().Wait();
                var destinationBlob = containerD.GetBlockBlobReference(blob.Name);
                
                if(await destinationBlob.ExistsAsync()){
                    //Console.WriteLine($"file exists {blob.Name}");
                    return; 
                }

                // Upload the compressed bytes to the new blob
                try{
                    await destinationBlob.UploadFromByteArrayAsync(compressedBytes, 0, compressedBytes.Length);
                }catch(Exception e){
                    var storageAccount = CloudStorageAccount.Parse(queueString);
                    var queueClient = storageAccount.CreateCloudQueueClient();
                    var queue = queueClient.GetQueueReference(queueName);
                    var message = new CloudQueueMessage(originalMessage);
                    await queue.AddMessageAsync(message);
                    return;
                }

                // Set the blob headers
                destinationBlob.Properties.ContentType = blob.Properties.ContentType;
                destinationBlob.Properties.ContentEncoding = "gzip";
                destinationBlob.SetProperties();
                
        }

    }
}
