using System.Runtime.InteropServices;

namespace CodecraftersKafka;

internal interface IResponseBody
{
    ReadOnlySpan<byte> ToSpan();
}

internal readonly record struct KafkaResponse<TBody>(ResponseHeader ResponseHeader, TBody Body)
    where TBody : struct, IResponseBody
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
        stream.Put((short)ErrorCode).Put((byte)(ApiVersions.Length + 1));
        
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
