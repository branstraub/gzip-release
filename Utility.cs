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
		public async Task EnsureGzipFiles(CloudBlobContainer containerS, CloudBlobContainer containerD, string fileName)
		{
			var blobInfo = containerS.GetBlobReference(fileName);
			await Upload(blobInfo, containerD);
		}

		public async Task Upload(IListBlobItem blobInfo, CloudBlobContainer containerD)
		{
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

			if (await destinationBlob.ExistsAsync())
			{
				//Console.WriteLine($"file exists {blob.Name}");
				return;
			}

			// Upload the compressed bytes to the new blob
			await destinationBlob.UploadFromByteArrayAsync(compressedBytes, 0, compressedBytes.Length);

			// Set the blob headers
			destinationBlob.Properties.ContentType = blob.Properties.ContentType;
			destinationBlob.Properties.ContentEncoding = "gzip";
			destinationBlob.SetProperties();
		}
	}
}
