using System.Runtime.InteropServices;
using System.Text;
using System.Linq;

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
        
        stream.Put(messageSize)
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

        var reqNameBytes = Request.Topics[0];

        var topics = MetadataLog.Batches.SelectMany(batch => batch.Records)
            .Where(record => record.RecordValue.Type == ClusterMetadataLog.RecordValueType.Topic
                && record.RecordValue.Topic.Name.Span.SequenceEqual(reqNameBytes.Span))
            .ToArray();
        
        Console.WriteLine($"Topics {topics.Length}: {string.Join(", ", topics)}");

        stream.Put((byte)0) // Suddenly the header has a TAG_BUFFER?
            .Put(2) // Throttle time
            .Put((byte)(topics.Length + 1)); // Array length (1)
        
        var errorCode = topics.Length != 0 ? ErrorCode.None : ErrorCode.UnknownTopicOrPartition;

        // We're branching here in order to keep passing the previous test-cases
        // which just mirrored the requests. 
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
            WriteTopicToRequest(stream, errorCode, Request.Topics[0], Guid.Empty.ToByteArray());
        }

        stream.Put(0x00000df8) // Topic authorised ops
            .Put((byte)0) // Damned TAG_BUFFER
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
            .Put((byte)(topicName.Length + 1)); // Lenght of topicname + 1, as 0 means null
        
        stream.Write(topicName.Span); // Topic name
        stream.Write(topicUuid.Span); // Topic Uuid

        stream.Put((byte)0) // Is internal
            .Put((byte)(partitions.Length+1)); // Positions length (0)
        
        Console.WriteLine($"Partition length: {partitions.Length}");

        var partId = 0;
        foreach (var partition in partitions)
        {
            var partitionRec = partition.RecordValue.Partition;
            Console.WriteLine($"Partition-{partId++}: {partitionRec.ToString()}");
            stream.Put((short)ErrorCode.None) // Errorcode
                .Put(partitionRec.PartitionId) // Partition Index
                .Put(partitionRec.Leader) // Leader ID
                .Put(partitionRec.LeaderEpoch) // Leader epoch
                .PutArray(partitionRec.ReplicaNodes) // Replica nodes
                .PutArray(partitionRec.InSync) // InSync nodes
                .PutArray(partitionRec.AddingReplicas) // ELR nodes
                .PutArray(partitionRec.RemovingReplicas) // offline nodes 
                .Put((byte)0); // Damned TAG_BUFFER
        }
    }
}
