using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using tusdotnet.Interfaces;
using tusdotnet.Models;
using tusdotnet.Stores;

namespace WebAPI.Helpers.Extensions
{

    public class CustomDiskStore: TusDiskStore, ITusStore, ITusCreationStore, ITusExpirationStore
    {
        private readonly string _directory;
        private readonly ITusFileIdProvider _fileIdProvider;
        // Use our own array pool to not leak data to other parts of the running app.
        private static readonly ArrayPool<byte> _bufferPool = ArrayPool<byte>.Create();
        private readonly int _maxReadBufferSize = 51200;
        private readonly int _maxWriteBufferSize = 51200;


        public CustomDiskStore(string directoryPath, ITusFileIdProvider fileIdProvider): base(directoryPath, true, TusDiskBufferSize.Default, fileIdProvider)
        {
            this._directory = directoryPath;
            this._fileIdProvider = fileIdProvider;
        }
        /// <inheritdoc />
        public async Task<string> CreateFileAsync(long uploadLength, string metadata, CancellationToken cancellationToken)
        {
            var fileId = await this._fileIdProvider.CreateId(metadata);
            var filePath = Path.Combine(this._directory, fileId);

            //upload file
            new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None).Dispose();

            //chunkcomplete
            var chunkCompleteFilePath = Path.Combine(this._directory, filePath + ".chunkcomplete");
            new FileStream(chunkCompleteFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None).Dispose();

            //chunkstart
            var chunkStartFilePath = Path.Combine(this._directory, filePath + ".chunkstart");
            new FileStream(chunkStartFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None).Dispose();

            //expiration
            var expirationFilePath = Path.Combine(this._directory, filePath + ".expiration");
            new FileStream(expirationFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None).Dispose();

            //uploadlength
            var uploadLengthFilePath = Path.Combine(this._directory, filePath + ".uploadlength");
            new FileStream(uploadLengthFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None).Dispose();

            if (uploadLength != -1)
            {
                using (StreamWriter textWriter = new StreamWriter(
                    new FileStream(uploadLengthFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None)))
                {
                    await textWriter.WriteAsync(uploadLength.ToString());
                }
            }

            var metaDataFileName = filePath + ".metadata";
            var metaDataFilePath = Path.Combine(this._directory, metaDataFileName);

            using (StreamWriter textWriter = new StreamWriter(
                    new FileStream(metaDataFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None)))
            {
                await textWriter.WriteAsync(metadata);
            }
            
            return fileId;
        }

        /// <inheritdoc />
        public async Task SetExpirationAsync(string fileId, DateTimeOffset expires, CancellationToken _)
        {
            var expirationFilePath = Path.Combine(this._directory, fileId + ".expiration");
            
            using (StreamWriter textWriter = new StreamWriter(
                    new FileStream(expirationFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None)))
            {
                await textWriter.WriteAsync(expires.ToString("O"));
            }
        }

        public async Task<long> AppendDataAsync(string fileId, Stream stream, CancellationToken cancellationToken)
        {
            var internalFilePath = Path.Combine(this._directory, fileId);

            var httpReadBuffer = _bufferPool.Rent(_maxReadBufferSize);
            var fileWriteBuffer = _bufferPool.Rent(Math.Max(_maxWriteBufferSize, _maxReadBufferSize));

            try
            {
                var fileUploadLengthProvidedDuringCreate = await GetUploadLengthAsync(fileId, cancellationToken);
                using var diskFileStream = new FileStream(internalFilePath, FileMode.Append, FileAccess.Write, FileShare.None, 4096, FileOptions.SequentialScan | FileOptions.Asynchronous);

                var totalDiskFileLength = diskFileStream.Length;
                if (fileUploadLengthProvidedDuringCreate == totalDiskFileLength)
                {
                    return 0;
                }

                InitializeChunk(fileId, totalDiskFileLength);

                int numberOfbytesReadFromClient;
                var bytesWrittenThisRequest = 0L;
                var clientDisconnectedDuringRead = false;
                var writeBufferNextFreeIndex = 0;

                do
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }

                    numberOfbytesReadFromClient = await stream.ReadAsync(httpReadBuffer, 0, _maxReadBufferSize, cancellationToken);
                    clientDisconnectedDuringRead = cancellationToken.IsCancellationRequested;

                    totalDiskFileLength += numberOfbytesReadFromClient;

                    if (totalDiskFileLength > fileUploadLengthProvidedDuringCreate)
                    {
                        throw new TusStoreException($"Stream contains more data than the file's upload length. Stream data: {totalDiskFileLength}, upload length: {fileUploadLengthProvidedDuringCreate}.");
                    }

                    // Can we fit the read data into the write buffer? If not flush it now.
                    if (writeBufferNextFreeIndex + numberOfbytesReadFromClient > _maxWriteBufferSize)
                    {
                        //write buffer to stream                                                
                        diskFileStream.Write(fileWriteBuffer, 0, writeBufferNextFreeIndex);
                        diskFileStream.Flush(true);

                        writeBufferNextFreeIndex = 0;
                    }

                    Array.Copy(
                        sourceArray: httpReadBuffer,
                        sourceIndex: 0,
                        destinationArray: fileWriteBuffer,
                        destinationIndex: writeBufferNextFreeIndex,
                        length: numberOfbytesReadFromClient);

                    writeBufferNextFreeIndex += numberOfbytesReadFromClient;
                    bytesWrittenThisRequest += numberOfbytesReadFromClient;

                } while (numberOfbytesReadFromClient != 0);

                // Flush the remaining buffer to disk.
                if (writeBufferNextFreeIndex != 0)
                {
                    //write buffer to stream
                    diskFileStream.Write(fileWriteBuffer, 0, writeBufferNextFreeIndex);
                    diskFileStream.Flush(true);
                }
                    
                if (!clientDisconnectedDuringRead)
                {
                    MarkChunkComplete(fileId);
                }

                return bytesWrittenThisRequest;
            }
            finally
            {
                _bufferPool.Return(httpReadBuffer);
                _bufferPool.Return(fileWriteBuffer);
            }
        }

        private void MarkChunkComplete(string fileId)
        {
            var chunkCompleteFilePath = Path.Combine(this._directory, fileId + ".chunkcomplete");
            using (StreamWriter textWriter = new StreamWriter(
                    new FileStream(chunkCompleteFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None)))
            {
                textWriter.Write("1");
            }
        }

        private void InitializeChunk(string fileId, long totalDiskFileLength)
        {
            //delete chunkcomplete file
            var chunkCompleteFilePath = Path.Combine(this._directory, fileId + ".chunkcomplete");
            if(File.Exists(chunkCompleteFilePath))
            {
                File.Delete(chunkCompleteFilePath);
            }

            //create chunk start file and write totalDiskFileLength
            var chunkStartFilePath = Path.Combine(this._directory, fileId + ".chunkstart");
            using (StreamWriter textWriter = new StreamWriter(
                    new FileStream(chunkStartFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None)))
            {
                textWriter.Write(totalDiskFileLength.ToString());
            }            
        }
    }
}
