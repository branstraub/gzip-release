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
        private long bytesBefore = 0;
        private long bytesAfter = 0;

        public async Task EnsureGzipFiles(CloudBlobContainer containerS, CloudBlobContainer containerD, string prefix, string queueString, string queueName)
        {
          
            var blobInfos = containerS.ListBlobs($"{prefix}/", true, BlobListingDetails.Metadata);
            //Console.WriteLine("Files: " + blobInfos.Count());

            //2018/april/ RECIBE
            //2018/april/30/eeee.json MAL
            //2018/april/eeee.json OK

            blobInfos = blobInfos.Where(x=>x.Uri.AbsolutePath.Substring(1, x.Uri.AbsolutePath.LastIndexOf('/')-1) == $"{containerS.Name}/{prefix}");
            Console.WriteLine("Files: " + blobInfos.Count());

            foreach(var blobInfo in blobInfos){
                 await Upload(blobInfo, containerD, queueName, queueString);
            }

            //Console.WriteLine("Size Before: " + bytesBefore / 1024 / 1024 + "mb");
            //Console.WriteLine("Size After: " + bytesAfter / 1024 / 1024 + "mb");
        }

        public async Task Upload(IListBlobItem blobInfo, CloudBlobContainer containerD, string queueName, string queueString){
            
                var blob = (CloudBlob)blobInfo; 
                byte[] compressedBytes;

                using (var memoryStream = new MemoryStream())
                {
                    using (var gzipStream = new GZipStream(memoryStream, CompressionMode.Compress))
                    using (var blobStream = await blob.OpenReadAsync())
                    {
                        await blobStream.CopyToAsync(gzipStream);
                        bytesBefore+=blobStream.Length;
                    }

                    compressedBytes = memoryStream.ToArray();
                    bytesAfter+=compressedBytes.Length;
                }

                containerD.CreateIfNotExistsAsync().Wait();
                var destinationBlob = containerD.GetBlockBlobReference(blob.Name);
                
                if(await destinationBlob.ExistsAsync()){
                    //Console.WriteLine($"file exists {blob.Name}");
                    return; 
                }

                // Upload the compressed bytes to the new blob
                Trace.TraceInformation("Writing blob: " + blob.Name);
                try{
                    await destinationBlob.UploadFromByteArrayAsync(compressedBytes, 0, compressedBytes.Length);
                }catch(Exception e){
                    var storageAccount = CloudStorageAccount.Parse(queueString);
                    var queueClient = storageAccount.CreateCloudQueueClient();
                    var queue = queueClient.GetQueueReference(queueName);
                    var message = new CloudQueueMessage(blob.Uri.AbsolutePath.Substring(1, blob.Uri.AbsolutePath.LastIndexOf('/')-1));
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
