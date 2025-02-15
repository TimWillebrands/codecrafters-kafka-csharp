using System.Runtime.InteropServices;
using System.Text;

namespace CodecraftersKafka;

internal interface IKafkaResponse
{
    ReadOnlySpan<byte> ToSpan();
}

internal interface IResponseBody
{
    ReadOnlySpan<byte> ToSpan();
}

internal readonly record struct KafkaResponse<TBody>(ResponseHeader ResponseHeader, TBody Body)
    : IKafkaResponse where TBody : struct, IResponseBody
{
    public ReadOnlySpan<byte> ToSpan()
    {
        using var stream = new MemoryStream();
        var bodySpan = Body.ToSpan();
        
        var sizeHeader = Marshal.SizeOf<ResponseHeader>();
        var sizeBody = bodySpan.Length;
        var messageSize = sizeHeader + sizeBody;
        
        stream
            .Put(messageSize)
            .Put(ResponseHeader.CorrelationId)
            .Write(bodySpan);
        
        Console.WriteLine($"Response message length: {messageSize}");
        
        return stream.ToArray();
    }
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct ResponseHeader(int CorrelationId);

internal readonly record struct ApiVersionsBody(
    ErrorCode ErrorCode, 
    ApiVersion[] ApiVersions) : IResponseBody
{
    public ReadOnlySpan<byte> ToSpan()
    {
        using var stream = new MemoryStream();
        stream.Put((short)ErrorCode)
            .Put((byte)(ApiVersions.Length + 1)); // num_api_keys in test, can't find it in spec.
        
        foreach (var av in ApiVersions)
        {
            stream.Put((short)av.ApiKey)
                .Put(av.MinVersion)
                .Put(av.MaxVersion)
                .Put((byte)0); // TAG BUFFER of this api_key 
        }

        stream.Put(0).Put((byte)0); // throttle time ms + TAG BUFFER of the api-keys body
        
        return stream.ToArray();
    }
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct ApiVersion(ApiKey ApiKey, short MinVersion, short MaxVersion);

internal readonly record struct DescribeTopicPartitionsBody(
    DescribeTopicPartitionsReqBody Request,
    ClusterMetadataLog MetadataLog) 
    : IResponseBody
{
    public ReadOnlySpan<byte> ToSpan()
    {
        using var stream = new MemoryStream();

        var reqNameBytes = Request.Topics;

        var topics = MetadataLog.Batches.SelectMany(batch => batch.Records)
            .Where(record => record.RecordValue.Type == ClusterMetadataLog.RecordValueType.Topic
                && reqNameBytes.Any(topic => 
                    record.RecordValue.Topic.Name.Span.SequenceEqual(topic.Span)))
            .ToArray();
        
        // We're branching here in order to keep passing the previous test-cases
        // which just mirrored the requests. 
        var topicLength = topics.Length == 0 ? 1 : topics.Length;
        Console.WriteLine($"Topics {topics.Length} ({topicLength}): {string.Join(", ", topics)}");

        stream.Put((byte)0) // Suddenly the header has a TAG_BUFFER?
            .Put(0) // Throttle time
            .Put(VarintDecoder.EncodeUnsignedVarint((uint)topicLength + 1)); // Array length (1)
        
        var errorCode = topics.Length != 0 ? ErrorCode.None : ErrorCode.UnknownTopicOrPartition;

        if (topics.Length != 0)
        {
            foreach (var topic in topics)
            {
                var (topicName, topicUuid) = topic.RecordValue.Topic;
                WriteTopicToRequest(stream, errorCode, topicUuid, topicName);
            }
        }
        else
        {
            WriteTopicToRequest(stream, errorCode, Guid.Empty.ToByteArray(),Request.Topics[0]);
        }

        stream
            .Put((byte)0xff) // Next Cursor
            .Put((byte)0); // Damned TAG_BUFFER
        
        var arr = stream.ToArray();
        return arr;
    }

    private void WriteTopicToRequest(
        MemoryStream stream, 
        ErrorCode errorCode,
        ReadOnlyMemory<byte> topicUuid, 
        ReadOnlyMemory<byte> topicName)
    {
        var partitions = MetadataLog.Batches.SelectMany(batch => batch.Records)
            .Where(record => record.RecordValue.Type == ClusterMetadataLog.RecordValueType.Partition
                             && record.RecordValue.Partition.TopicUuid.Span.SequenceEqual(topicUuid.Span))
            .ToArray();
            
        stream
            .Put((short)errorCode)
            .Put(VarintDecoder.EncodeUnsignedVarint((uint)topicName.Length + 1)); // Lenght of topicname + 1, as 0 means null
        
        stream.Write(topicName.Span); // Topic name
        stream.Write(topicUuid.Span); // Topic Uuid

        stream.Put((byte)0) // Is internal
            .Put((byte)(partitions.Length+1)); // Positions length (0)
        
        foreach (var partition in partitions)
        {
            var partitionRec = partition.RecordValue.Partition;
            
            stream.Put((short)ErrorCode.None) // Errorcode
                .Put(partitionRec.PartitionId) // Partition Index
                .Put(partitionRec.Leader) // Leader ID
                .Put(partitionRec.LeaderEpoch) // Leader epoch
                .PutCompactArray(partitionRec.ReplicaNodes) // Replica nodes TODO: I think we're reading this thing wrong?
                .PutCompactArray(partitionRec.InSync) // InSync nodes
                .PutCompactArray(partitionRec.AddingReplicas) // ELR nodes
                .PutCompactArray(partitionRec.AddingReplicas) // last_known_elr 
                .PutCompactArray(partitionRec.RemovingReplicas) // offline nodes 
                .Put((byte)0); // Damned TAG_BUFFER
        }

        stream
            .Put(0x00000df8) // Topic authorised ops
            .Put((byte)0); // Damned TAG_BUFFER
    }
}
internal readonly record struct FetchBody(
    FetchReqBody Request,
    ClusterMetadataLog MetadataLog) : IResponseBody
{
    public ReadOnlySpan<byte> ToSpan()
    {
        using var stream = new MemoryStream();
        stream
            .Put((byte)0) // Damned TAG_BUFFER
            .Put(0) // Throttle time
            .Put((short)ErrorCode.None) // Error code
            .Put(0) // Session id
            // responses => topic_id [partitions] TAG_BUFFER 
            .Put(VarintDecoder.EncodeUnsignedVarint((uint)(Request.Topics?.Length + 1 ?? 0))); // Length of compact arr is an unsigned-varint
        
        for (var index = 0; index < (Request.Topics?.Length ?? 1); index++)
        {
            var request = Request.Topics![index];
            var topics = MetadataLog.Batches.SelectMany(batch => batch.Records)
                .Where(record => record.RecordValue.Type == ClusterMetadataLog.RecordValueType.Topic
                    && record.RecordValue.Topic.Uuid.Span.SequenceEqual(request.TopicId.Span))
                .ToArray();

            if (topics.Length > 1)
            {
                throw new NotSupportedException("I'm not entirely sure this is even valid, there are more than one topics of that id?");
            }
            
            var errorCode = topics.Length != 0 ? ErrorCode.None : ErrorCode.UnknownTopicId;
            
            var partitions = MetadataLog.Batches
                .SelectMany(batch => batch.Records)
                .FirstOrDefault(record => record.RecordValue.Type == ClusterMetadataLog.RecordValueType.Partition 
                                          && record.RecordValue.Partition.TopicUuid.Span.SequenceEqual(request.TopicId.Span));

            stream
                // The topic_id field matches what was sent in the request.
                .Put(request.TopicId)
                // The partitions array length
                // TODO: Deze code is denk ik te _DRY_, bij UNKNOWN_TOPIC is er geen
                //       partition gevonden, maar we moeten er wel een returnen met
                //       fixed values. Ik denk dat ik dit moet refactoren.
                .Put(VarintDecoder.EncodeUnsignedVarint(2));
            
            stream
                // var partition = partitions[i];
                // The partition_index field is 0.
                .Put(0)
                // The error_code field is 100 (UNKNOWN_TOPIC).
                .Put((short)errorCode)
                //     high_watermark => INT64
                .Put((long)0)
                //     last_stable_offset => INT64
                .Put((long)0)
                //     log_start_offset => INT64
                .Put((long)0)
                //     aborted_transactions => producer_id first_offset TAG_BUFFER 
                .Put(VarintDecoder.EncodeUnsignedVarint(0))
                //     preferred_read_replica => INT32
                .Put(-1);

            if (topics.Length > 0)
            {
                var topicName = Encoding.UTF8.GetString(topics.FirstOrDefault().RecordValue.Topic.Name.Span);
                var recordBatchesRaw = ClusterMetadataLog.ReadPartitionRecordsRaw(topicName, "0");
                var recordBatches = ClusterMetadataLog.ReadPartitionRecords(topicName, "0");
                
                // Create a temporary stream to hold the record batches
                using var recordsStream = new MemoryStream();
                foreach (var recordBatchRaw in recordBatchesRaw)
                {
                    recordsStream.Write(recordBatchRaw.Span);
                }

                // Get the total length of the serialized record batches
                // var recordsLength = (uint) recordsStream.Length;
                var recordsLength = (uint) recordBatches.Aggregate(1, (agg, batch) => agg + batch.BatchLength);

                // Write the records_length (as varint) to the main stream
                stream.Put(VarintDecoder.EncodeUnsignedVarint(recordsLength));

                // Write the record batches from the temporary stream to the main stream
                recordsStream.Position = 0; // Reset stream position to the beginning
                recordsStream.CopyTo(stream);

            }
            else
            {
                stream.Put((byte)0); // Damned TAG_BUFFER
            }
            stream.Put((byte)0); // Damned TAG_BUFFER
            stream.Put((byte)0); // Damned TAG_BUFFER
        }
        stream.Put((byte)0); // Damned TAG_BUFFER
        
        return stream.ToArray();
    }
}