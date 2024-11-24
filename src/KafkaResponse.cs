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
        
        stream.Put(messageSize)
            .Put(ResponseHeader.CorrelationId)
            .Write(bodySpan);
        
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
            stream.Put((short)av.ApiKey).Put(av.MinVersion).Put(av.MaxVersion)
                .Put((byte)0); // TAG BUFFER of this api_key 
        }

        stream.Put(0).Put((byte)0); // throttle time ms + TAG BUFFER of the api-keys body
        
        return stream.ToArray();
    }
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct ApiVersion(ApiKey ApiKey, short MinVersion, short MaxVersion);

internal readonly record struct DescribeTopicPartitionsBody(DescribeTopicPartitionsReqBody Request) 
    : IResponseBody
{
    public ReadOnlySpan<byte> ToSpan()
    {
        using var stream = new MemoryStream();
        stream.Put((byte)0) // Suddenly the header has a TAG_BUFFER?
            .Put(0) // Throttle time
            .Put((byte)2) // Array length (1)
            .Put((short)ErrorCode.UnknownTopicOrPartition)
            .Put((byte) (Request.Topics[0].Length + 1)); // Lenght of topicname + 1, as 0 means null
        
        stream.Write(Request.Topics[0].Span) ;
        stream.Write(Guid.Empty.ToByteArray());
        
        stream.Put((byte)0) // Is internal
            .Put((byte)1) // Positions length (0)
            .Put(0x00000df8) // Topic authorised ops
            .Put((byte)0) // Damned TAG_BUFFER
            .Put((byte)0xff)
            .Put((byte)0); // Damned TAG_BUFFER
        
        return stream.ToArray();
    }
}
