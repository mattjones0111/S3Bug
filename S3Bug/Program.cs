using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace S3Bug
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            IContainer container = null;

            try
            {
                container = new ContainerBuilder()
                    .WithImage("localstack/localstack:latest")  // <-- doesn't work
                    //.WithImage("localstack/localstack:2.2.0") // <-- works
                    .WithPortBinding(4566, true)
                    .WithCleanUp(true)
                    .Build();

                await container.StartAsync();

                int mappedPort = container.GetMappedPublicPort(4566);

                var client = new DefaultS3Client(mappedPort);

                var stream = typeof(Program)
                    .Assembly
                    .GetManifestResourceStream("S3Bug.Data.random.json");

                await client.UploadAsync("bucket-name", "random.json", stream);

                Console.WriteLine("Success!");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed :-( " + ex.Message);
            }
            finally
            {
                if (container != null)
                {
                    await container.DisposeAsync();
                }
            }
        }
    }

    public class DefaultS3Client
    {
        readonly AmazonS3Client client;

        public DefaultS3Client(int port)
        {
            client = new AmazonS3Client(
                new BasicAWSCredentials("xxx", "xxx"),
                new AmazonS3Config
                {
                    ServiceURL = $"http://localhost:{port}",
                    ForcePathStyle = true
                });
        }

        public async Task EnsureBucketCreatedAsync(string bucketName)
        {
            await client.PutBucketAsync(bucketName);
        }

        public async Task UploadAsync(
            string bucketName,
            string name,
            Stream stream,
            string contentType = "application/json",
            CancellationToken cancellationToken = default)
        {
            await EnsureBucketCreatedAsync(bucketName);

            var initRequest = new InitiateMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = name
            };

            if (!string.IsNullOrWhiteSpace(contentType))
            {
                initRequest.ContentType = contentType;
            }

            var initResponse = await client.InitiateMultipartUploadAsync(
                initRequest,
                cancellationToken);

            var uploadResponses = new List<UploadPartResponse>();

            try
            {
                bool done = false;
                int part = 1;

                while (!done)
                {
                    var partStream = GetChunk(stream);

                    if (partStream.Length == 0)
                    {
                        done = true;
                        continue;
                    }

                    var partRequest = new UploadPartRequest
                    {
                        BucketName = bucketName,
                        Key = name,
                        PartNumber = part,
                        InputStream = partStream,
                        UploadId = initResponse.UploadId
                    };

                    var partResponse = await client.UploadPartAsync(
                        partRequest,
                        cancellationToken);

                    uploadResponses.Add(partResponse);

                    part++;
                }
            }
            catch (Exception)
            {
                await client.AbortMultipartUploadAsync(
                    bucketName,
                    name,
                    initResponse.UploadId,
                    cancellationToken);

                throw;
            }

            var completeRequest = new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = name,
                UploadId = initResponse.UploadId,
                PartETags = uploadResponses
                    .Select(x => new PartETag
                    {
                        ETag = x.ETag,
                        PartNumber = x.PartNumber
                    })
                    .OrderBy(x => x.PartNumber)
                    .ToList()
            };

            await client.CompleteMultipartUploadAsync(
                completeRequest,
                cancellationToken);
        }

        private static MemoryStream GetChunk(
            Stream stream,
            int chunkSizeInMegabytes = 10)
        {
            var result = new MemoryStream();
            long bytesWritten = 0;
            bool done = false;
            byte[] bytes = new byte[4096];

            while (!done)
            {
                int bytesRead = stream.Read(bytes, 0, 4096);
                result.Write(bytes, 0, bytesRead);
                bytesWritten += bytesRead;

                if (bytesRead == 0 ||
                    bytesWritten >= chunkSizeInMegabytes * 1024 * 1024)
                {
                    done = true;
                }
            }

            result.Seek(0L, SeekOrigin.Begin);
            return result;
        }
    }
}
