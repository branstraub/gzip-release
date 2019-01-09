﻿using System;
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

			var storageAccountQueue = CloudStorageAccount.Parse(options.QueueConnectionString);
			var queueClient = storageAccountQueue.CreateCloudQueueClient();
			var queue = queueClient.GetQueueReference(options.QueueName);

			var stopWatch = new Stopwatch();
			stopWatch.Start();
			var msg = ".";

			//msg = "https://gzipo.blob.core.windows.net/gzipi/2018/april/0/ac583c70-df76-4e02-a70f-7a3d06a0985d.json";

			while (msg != "null")
			{

				var retrievedMessage = await queue.GetMessageAsync();

				if (retrievedMessage != null)
				{
					msg = retrievedMessage.AsString;
					await queue.DeleteMessageAsync(retrievedMessage);
				}
				else
				{
					msg = "null";
					return;
				}

				var container = msg.Split('/')[3];
				var fileName = string.Join("/", msg.Split('/').Skip(4));
				var yearAndMonth = fileName.Substring(0, 6);
				var storageAccountS = CloudStorageAccount.Parse(options.BlobConnectionStringSource);
				var storageAccountD = CloudStorageAccount.Parse(options.BlobConnectionStringDestination);

				try
				{
					var blobClientS = storageAccountS.CreateCloudBlobClient();
					var blobContainerS = blobClientS.GetContainerReference(container);

					var blobClientD = storageAccountD.CreateCloudBlobClient();
					var blobContainerD = blobClientD.GetContainerReference(container + yearAndMonth + "compressed");

					// Do the compression work
					await new Utility().EnsureGzipFiles(blobContainerS, blobContainerD, fileName, options.QueueName, options.QueueConnectionString, msg);
				}
				catch (Exception)
				{
					var storageAccount = CloudStorageAccount.Parse(options.QueueConnectionString);
					queueClient = storageAccount.CreateCloudQueueClient();
					queue = queueClient.GetQueueReference(options.QueueName);
					var message = new CloudQueueMessage(msg);
					await queue.AddMessageAsync(message);
					// Todo write this message to a file in the C disk
				}
			}

			stopWatch.Stop();
			var ts = stopWatch.Elapsed;
		}
	}
}
