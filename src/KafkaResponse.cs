using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace CodecraftersKafka;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct KafkaResponse(int MessageSize, Header Header)
{
    public ReadOnlySpan<byte> ToSpan()
    {
        var span = new Span<byte>(new byte[Marshal.SizeOf<KafkaResponse>()]);

        BinaryPrimitives.WriteInt32BigEndian(span[..4], MessageSize);
        BinaryPrimitives.WriteInt32BigEndian(span[4..8], Header.CorrelationId);

        return span;
    }
    
    public static KafkaResponse FromSpan(ReadOnlySpan<byte> span)
    {
        if (span.Length < Marshal.SizeOf<KafkaResponse>())
            throw new ArgumentException("Span is too small for KafkaResponse.");

        var messageSize = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var correlationId = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);

        return new KafkaResponse(messageSize, new Header(correlationId)); 
    }
};

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct Header(int CorrelationId);