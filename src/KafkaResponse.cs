using System.Buffers.Binary;
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
        var bodySpan = Body.ToSpan();
        
        var sizeHeader = Marshal.SizeOf<ResponseHeader>();
        var sizeBody = bodySpan.Length;
        var messageSize = sizeHeader + sizeBody;
            
        var span = new Span<byte>(new byte[messageSize + sizeof(int)]);
        
        BinaryPrimitives.WriteInt32BigEndian(span[..4], messageSize);
        BinaryPrimitives.WriteInt32BigEndian(span[4..8], ResponseHeader.CorrelationId);
        
        bodySpan.CopyTo(span[8..]);

        return span;
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
        const int size = 3 * sizeof(short) + 1;
        
        var messageSize = 
            sizeof(ErrorCode) 
            + 1 // This is num_of_api_keys in the test... I can't find it in the spec 
            + ApiVersions.Length*size
            + sizeof(int) // throttle time ms
            + 1; // tag buffer
        var result = new Span<byte>(new byte[messageSize]);
        
        BinaryPrimitives.WriteInt16BigEndian(result[..2], (short)ErrorCode); // error_code
        result[2] = (byte) (ApiVersions.Length + 1);
        for (var av = 0; av < ApiVersions.Length; av++)
        {
            var i = 3 + av * size;
            BinaryPrimitives.WriteInt16BigEndian(result[(i+0)..(i+2)], (short)ApiVersions[av].ApiKey);
            BinaryPrimitives.WriteInt16BigEndian(result[(i+2)..(i+4)], ApiVersions[av].MinVersion);
            BinaryPrimitives.WriteInt16BigEndian(result[(i+4)..(i+6)], ApiVersions[av].MaxVersion);
            result[i + 6] = 0; // TAG BUFFER of this api_key 
        }

        BinaryPrimitives.WriteInt32BigEndian(result[^5..^1], 0); // throttle time ms
        result[^1] = 0; // TAG BUFFER of the api-keys body
        
        return result;
    }
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct ApiVersion(ApiKey ApiKey, short MinVersion, short MaxVersion);
