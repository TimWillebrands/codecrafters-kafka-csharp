using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace CodecraftersKafka;

internal interface IResponseBody
{
    ReadOnlySpan<byte> ToSpan();
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct KafkaResponse<TBody>(
    int MessageSize, 
    ResponseHeader ResponseHeader, 
    TBody Body) where TBody : struct, IResponseBody
{
    public ReadOnlySpan<byte> ToSpan()
    {
        var sizeHeader = Marshal.SizeOf<ResponseHeader>();
        var sizeBody = Marshal.SizeOf<TBody>();
        var span = new Span<byte>(new byte[4 + sizeHeader + sizeBody]);

        BinaryPrimitives.WriteInt32BigEndian(span[..4], MessageSize);
        BinaryPrimitives.WriteInt32BigEndian(span[4..8], ResponseHeader.CorrelationId);
        
        Body.ToSpan().CopyTo(span[8..]);

        return span;
    }
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct ResponseHeader(int CorrelationId);


[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct ApiVersionsBody(ErrorCode ErrorCode) : IResponseBody
{
    public ReadOnlySpan<byte> ToSpan()
    {
        var span = new Span<byte>(new byte[Marshal.SizeOf<ApiVersionsBody>()]);

        BinaryPrimitives.WriteInt16BigEndian(span[..2], (short)ErrorCode);
        
        return span;
    }
};
