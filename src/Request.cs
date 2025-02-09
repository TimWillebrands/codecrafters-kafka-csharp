using System.Buffers.Binary;
using System.Text;

namespace CodecraftersKafka;

internal interface IKafkaRequest
{
    int MessageSize { get; } 
    RequestHeader Header { get; }
}

internal static class Request
{
    /// <summary>
    /// Create a view on the incoming bytes
    /// </summary>
    /// <param name="buffer">The bytes of the request, expected to be temporarily allocated</param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException">If the api-key is unknown</exception>
    public static IKafkaRequest FromSpan(ReadOnlyMemory<byte> buffer)
    {
        var offset = 0;
        var messageSize = buffer.ReadInt(ref offset, "message_size");
        var apiKey = (ApiKey)buffer.ReadShort(ref offset, "api_key");
        var apiKeyVersion = buffer.ReadShort(ref offset, "api_key_version");
        var correlationId = buffer.ReadInt(ref offset, "correlation_id");
        
        var header = new RequestHeader(apiKey, apiKeyVersion, correlationId);

        return apiKey switch
        {
            ApiKey.ApiVersions => new ApiVersionsReqBody(messageSize, header),
            ApiKey.DescribeTopicPartitions => 
                new DescribeTopicPartitionsReqBody(messageSize, header, buffer[12..]),
            ApiKey.Fetch => new FetchReqBody(messageSize, header, buffer[12..]),
            _ => throw new NotImplementedException($"Unknown API key: {apiKey}"),
        };
    }
}

// 00 00 00 31 00 4b 00 00 22 60 a0 91 00 0c 6b 61
// 66 6b 61 2d 74 65 73 74 65 72 00 02 12 75 6e 6b
// 6e 6f 77 6e 2d 74 6f 70 69 63 2d 73 61 7a 00 00
// 00 00 01 ff 00

internal readonly record struct RequestHeader(ApiKey ApiKey, short ApiKeyVersion, int CorrelationId);

internal readonly record struct ApiVersionsReqBody : IKafkaRequest
{
    internal ApiVersionsReqBody (int messageSize, RequestHeader header)
    {
        MessageSize = messageSize;
        Header = header;
    }

    public int MessageSize { get; }
    public RequestHeader Header { get; }
}

internal readonly record struct DescribeTopicPartitionsReqBody : IKafkaRequest
{
    internal DescribeTopicPartitionsReqBody (int messageSize, RequestHeader header, ReadOnlyMemory<byte> buffer)
    {
        MessageSize = messageSize;
        Header = header;

        var offset = 0;
        ClientId = ParseUtils.GetStr(buffer, ref offset, 2);
        offset++; // TAG_BUFER
        var topicsLen = VarintDecoder.ReadUnsignedVarint(buffer.Span, ref offset);
        Topics = new ReadOnlyMemory<byte>[topicsLen - 1];
        for (var i = 0; i < Topics.Length; i++)
        {
            // I think this is a mistake in the tests... why is the length suddenly only 1 byte
            // and if you use this length you will include the TAG_BUFFER as well and add a 0
            // byte to the end of our string.
            Topics[i] = ParseUtils.GetStr(buffer, ref offset, 1)[..^1];
            // offset++; // TAG_BUFER
        }
        ResponsePartitionLimit = BinaryPrimitives.ReadInt32BigEndian(buffer.Span[offset..(offset+=4)]);
        Cursor = buffer.Span[offset];
        offset++; // TAG_BUFER
    }
    
    public int MessageSize { get; }
    public RequestHeader Header { get; }
    public ReadOnlyMemory<byte> ClientId { get; }
    public ReadOnlyMemory<byte>[] Topics { get; }
    public int ResponsePartitionLimit { get; }
    public byte Cursor { get; }
}

internal readonly record struct FetchReqBody : IKafkaRequest
{
    internal FetchReqBody (int messageSize, RequestHeader header, ReadOnlyMemory<byte> buffer)
    {
        MessageSize = messageSize;
        Header = header;
        var offset = 0;
        
        // TODO: Find out if this should be part of the header?
        //       From v1 onwards it seems to be: https://kafka.apache.org/protocol.html#protocol_messages
        var clientId = buffer.ReadNullableString(ref offset, "client_id");
        offset++; // Pour one out for the TAG_BUFFER
        
        // max_wait_ms => INT32
        MaxWaitMs = buffer.ReadInt(ref offset, "max_wait_ms");
        // min_bytes => INT32
        MinBytes = buffer.ReadInt(ref offset, "min_bytes");
        // max_bytes => INT32
        MaxBytes = buffer.ReadInt(ref offset, "max_bytes");
        // isolation_level => INT8
        IsolationLevel = buffer.ReadByte(ref offset, "isolation_level");
        // session_id => INT32
        SessionId = buffer.ReadInt(ref offset, "session_id");
        // session_epoch => INT32
        SessionEpoch = buffer.ReadInt(ref offset, "session_epoch");
        
        // topics => topic_id [partitions] TAG_BUFFER 
        var topicsLen = buffer.ReadVarUInt(ref offset, "topics_length");

        if (topicsLen == 0)
        {
            return;
        }
        
        Topics = new Topic[topicsLen - 1];
        
        Console.WriteLine($"[Fetch Req Body] ");

        for (var i = 0; i < Topics.Length; i++)
        {
            Topics[i] = new Topic 
            {
                // topic_id => UUID
                TopicId = buffer[offset..(offset+=16)],
                // partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes TAG_BUFFER 
                Partitions = new Partition[buffer.ReadVarUInt(ref offset) - 1],                
            };

            Console.WriteLine($"Topic-{i}/{Topics.Length} | {Topics[i].TopicIdAsGuid}");
            
            for (var j = 0; j < Topics[i].Partitions.Length; j++)
            {
                Topics[i].Partitions[j] = new Partition
                {
                    // partition => INT32
                    PartitionId = buffer.ReadInt(ref offset, "partition"),
                    // current_leader_epoch => INT32
                    CurrentLeaderEpoch = buffer.ReadInt( ref offset, "current_leader_epoch"),
                    // fetch_offset => INT64
                    FetchOffset = buffer.ReadLong( ref offset, "fetch_offset"),
                    // last_fetched_epoch => INT32
                    LastFetchedEpoch = buffer.ReadInt( ref offset, "last_fetched_epoch"),
                    // log_start_offset => INT64
                    LastStartOffset = buffer.ReadLong( ref offset, "log_start_offset"),
                    // partition_max_bytes => INT32 
                    PartitionMaxBytes = buffer.ReadInt( ref offset, "partition_max_bytes"),
                };
            }
        }
        // TODO: I'm pretty sure I won't be using these fields, so for now I guess I'll just
        //       let this memory rot until GC comes :)
        // forgotten_topics_data => topic_id [partitions] TAG_BUFFER 
        //     topic_id => UUID
        //     partitions => INT32
        // rack_id => COMPACT_STRING
    }

    public RequestHeader Header { get; }
    public int MessageSize { get; }
    public int MaxWaitMs { get; }
    public int MinBytes { get; }
    public int MaxBytes { get; }
    public byte IsolationLevel { get; }
    public int SessionId { get; }
    public int SessionEpoch { get; }
    
    public Topic[]? Topics { get; }

    public readonly record struct Topic
    {
        public ReadOnlyMemory<byte> TopicId { get; init; }
        public Guid TopicIdAsGuid => new Guid(TopicId.Span);
        public Partition[] Partitions { get; init; }
    }

    public readonly record struct Partition 
    {
        public int PartitionId { get; init; }
        public int CurrentLeaderEpoch { get; init; }
        public long FetchOffset { get; init; }
        public int LastFetchedEpoch { get; init; }
        public long LastStartOffset { get; init; }
        public int PartitionMaxBytes { get; init; }
    }
}

internal static class ParseUtils {
    internal static ReadOnlyMemory<byte> GetStr(ReadOnlyMemory<byte> buffer, ref int offset, int lenSz)
    {
        var len = lenSz switch
        {
            1 => buffer.Span[offset++],
            2 => BinaryPrimitives.ReadInt16BigEndian(buffer.Span[offset..(offset += lenSz)]),
            _ => throw new ArgumentOutOfRangeException(nameof(lenSz), lenSz, "Invalid length")
        }; // -1 because varint, thus 0 is null
        var str = buffer[offset..(offset += len)];
        
        Console.WriteLine($"[str] {len} - '{Encoding.UTF8.GetString(str.Span)}' - {string.Join(',', str.Span.ToArray())}");
        return str;
    }
}