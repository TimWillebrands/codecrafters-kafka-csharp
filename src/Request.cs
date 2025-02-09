using System.Buffers.Binary;
using System.Runtime.InteropServices;
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
        var span = buffer.Span;
        var messageSize = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var apiKey = (ApiKey) BinaryPrimitives.ReadInt16BigEndian(span[4..6]);
        var apiKeyVersion = BinaryPrimitives.ReadInt16BigEndian(span[6..8]);
        var correlationId = BinaryPrimitives.ReadInt32BigEndian(span[8..12]);
        
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

internal readonly record struct FetchReqBody(
    int MessageSize, 
    RequestHeader Header, 
    ReadOnlyMemory<byte> ReadOnlyMemory) : IKafkaRequest
{
    public int MessageSize { get; }
    public RequestHeader Header { get; }
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